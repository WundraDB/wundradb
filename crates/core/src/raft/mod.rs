use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Term(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LogIndex(pub u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub command: Vec<u8>,
    pub id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
    pub match_index: LogIndex,
}

#[derive(Debug)]
pub struct RaftNode {
    pub id: NodeId,
    pub state: NodeState,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub peers: Vec<NodeId>,
    pub leader_id: Option<NodeId>,
    pub next_index: HashMap<NodeId, LogIndex>,
    pub match_index: HashMap<NodeId, LogIndex>,
    pub last_heartbeat: Instant,
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub command_sender: mpsc::UnboundedSender<Vec<u8>>,
    pub command_receiver: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();

        Self {
            id,
            state: NodeState::Follower,
            current_term: Term(0),
            voted_for: None,
            log: Vec::new(),
            commit_index: LogIndex(0),
            last_applied: LogIndex(0),
            peers,
            leader_id: None,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            last_heartbeat: Instant::now(),
            election_timeout: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(100),
            command_sender: tx,
            command_receiver: rx,
        }
    }

    pub fn is_leader(&self) -> bool {
        self.state == NodeState::Leader
    }

    pub fn is_follower(&self) -> bool {
        self.state == NodeState::Follower
    }

    pub fn start_election(&mut self) {
        self.current_term.0 += 1;
        self.state = NodeState::Candidate;
        self.voted_for = Some(self.id.clone());
        self.last_heartbeat = Instant::now();
    }

    pub fn become_leader(&mut self) {
        self.state = NodeState::Leader;
        self.leader_id = Some(self.id.clone());

        let next_index = LogIndex(self.get_last_log_index().0 + 1);
        for peer in &self.peers {
            self.next_index.insert(peer.clone(), next_index);
            self.match_index.insert(peer.clone(), LogIndex(0));
        }
    }

    pub fn get_last_log_index(&self) -> LogIndex {
        self.log.last().map(|e| e.index).unwrap_or(LogIndex(0))
    }

    pub fn get_last_log_term(&self) -> Term {
        self.log.last().map(|e| e.term).unwrap_or(Term(0))
    }

    pub fn handle_vote_request(&mut self, req: VoteRequest) -> VoteResponse {
        let mut granted = false;

        if req.term > self.current_term {
            self.current_term = req.term;
            self.voted_for = None;
            self.state = NodeState::Follower;
        }

        if req.term == self.current_term
            && (self.voted_for.is_none() || self.voted_for.as_ref() == Some(&req.candidate_id))
            && self.is_log_up_to_date(req.last_log_index, req.last_log_term)
        {
            granted = true;
            self.voted_for = Some(req.candidate_id);
        }

        VoteResponse {
            term: self.current_term,
            vote_granted: granted,
        }
    }

    pub fn handle_append_entries(&mut self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        if req.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
                match_index: LogIndex(0),
            };
        }

        self.leader_id = Some(req.leader_id.clone());
        self.state = NodeState::Follower;
        self.last_heartbeat = Instant::now();
        self.current_term = req.term;

        AppendEntriesResponse {
            term: self.current_term,
            success: true,
            match_index: self.get_last_log_index(),
        }
    }

    fn is_log_up_to_date(&self, index: LogIndex, term: Term) -> bool {
        let my_term = self.get_last_log_term();
        let my_index = self.get_last_log_index();
        term > my_term || (term == my_term && index >= my_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_node_init() {
        let id = NodeId("n1".into());
        let peers = vec![NodeId("n2".into()), NodeId("n3".into())];
        let node = RaftNode::new(id.clone(), peers.clone());

        assert_eq!(node.id, id);
        assert_eq!(node.peers, peers);
        assert_eq!(node.current_term, Term(0));
        assert_eq!(node.state, NodeState::Follower);
    }

    #[test]
    fn test_start_election() {
        let id = NodeId("n1".into());
        let mut node = RaftNode::new(id.clone(), vec![]);
        node.start_election();

        assert_eq!(node.state, NodeState::Candidate);
        assert_eq!(node.voted_for, Some(id));
        assert_eq!(node.current_term, Term(1));
    }

    #[test]
    fn test_vote_granted() {
        let mut node = RaftNode::new(NodeId("n1".into()), vec![]);
        let req = VoteRequest {
            term: Term(1),
            candidate_id: NodeId("n2".into()),
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
        };
        let res = node.handle_vote_request(req);
        assert!(res.vote_granted);
    }
}
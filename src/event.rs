use std::{iter::repeat, sync::Arc};

use iroh::{NodeId, endpoint::StreamId};
use polestar::id::IdMap;
use tokio::sync::Mutex;

use crate::matheus::Connections;

#[derive(Debug)]
pub struct Event<N, C, S> {
    node: N,
    remote: N,
    conn: C,
    event: EventType<S>,
}

pub type EventSystem = Event<NodeId, u64, iroh::endpoint::StreamId>;
pub type EventModel = Event<Nid, Cid, Sid>;

impl<N, C, S> std::fmt::Display for Event<N, C, S>
where
    N: std::fmt::Display,
    C: std::fmt::Display,
    S: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let node = &self.node;
        let to = &self.remote;
        let conn = &self.conn;
        match &self.event {
            EventType::OpenConnection => {
                write!(f, "{:>16} : {node} -> {to} : {conn}", "OpenConnection")
            }
            EventType::AcceptConnection => {
                write!(f, "{:>16} : {node} <- {to} : {conn}", "AcceptConnection")
            }
            EventType::CloseConnection => {
                write!(f, "{:>16} : {node} xx {to} : {conn}", "CloseConnection")
            }
            EventType::OpenStream { stream } => {
                write!(
                    f,
                    "{:>16} : {node} ~> {to} : {conn} . {stream}",
                    "OpenStream"
                )
            }
            EventType::ReadStream { stream } => {
                write!(
                    f,
                    "{:>16} : {node} ~~ {to} : {conn} . {stream}",
                    "ReadStream"
                )
            }
            EventType::WriteStream { stream } => {
                write!(
                    f,
                    "{:>16} : {node} ~~ {to} : {conn} . {stream}",
                    "WriteStream"
                )
            }
            EventType::FinishStream { stream } => {
                write!(
                    f,
                    "{:>16} : {node} ~~ {to} : {conn} . {stream}",
                    "FinishStream"
                )
            }
            EventType::AcceptStream { stream } => {
                write!(
                    f,
                    "{:>16} : {node} <~ {to} : {conn} . {stream}",
                    "AcceptStream"
                )
            }
            EventType::EndStream { stream } => {
                write!(
                    f,
                    "{:>16} : {node} xx {to} : {conn} . {stream}",
                    "EndStream"
                )
            }
            EventType::Error { err } => {
                write!(f, "{:>16} : {node} XX {to} : {conn} : {err}", "Error")
            }
        }
    }
}
impl<N, C, S> Event<N, C, S> {
    pub fn new(node: impl Into<N>, remote: impl Into<N>, conn: C, event: EventType<S>) -> Self {
        let node = node.into();
        let remote = remote.into();
        Self {
            node,
            remote,
            conn,
            event,
        }
    }
}

pub type EventTypeSystem = EventType<iroh::endpoint::StreamId>;

#[derive(Debug)]
pub enum EventType<S> {
    OpenConnection,
    AcceptConnection,
    CloseConnection,
    OpenStream {
        stream: S,
    },
    AcceptStream {
        stream: S,
    },
    WriteStream {
        stream: S,
    },
    ReadStream {
        stream: S,
    },
    FinishStream {
        stream: S,
    },
    EndStream {
        stream: S,
    },

    Error {
        // stream: Option<S>,
        err: String,
    },
}

impl<N, C, S> Event<N, C, S> {
    pub fn map_nodes<X>(mut self, mut f: impl FnMut(N) -> X) -> Event<X, C, S> {
        let node = f(self.node);
        let remote = f(self.remote);
        Event {
            node,
            remote,
            conn: self.conn,
            event: self.event,
        }
    }

    pub fn map_conns<X>(self, mut f: impl FnMut(C) -> X) -> Event<N, X, S> {
        let conn = f(self.conn);
        Event {
            node: self.node,
            remote: self.remote,
            conn,
            event: self.event,
        }
    }

    pub fn map_streams<X>(self, mut f: impl FnMut(S) -> X) -> Event<N, C, X> {
        let event = match self.event {
            EventType::OpenConnection => EventType::OpenConnection,
            EventType::AcceptConnection => EventType::AcceptConnection,
            EventType::CloseConnection => EventType::CloseConnection,
            EventType::OpenStream { stream } => EventType::OpenStream { stream: f(stream) },
            EventType::AcceptStream { stream } => EventType::AcceptStream { stream: f(stream) },
            EventType::WriteStream { stream } => EventType::WriteStream { stream: f(stream) },
            EventType::ReadStream { stream } => EventType::ReadStream { stream: f(stream) },
            EventType::FinishStream { stream } => EventType::FinishStream { stream: f(stream) },
            EventType::EndStream { stream } => EventType::EndStream { stream: f(stream) },
            EventType::Error { err } => EventType::Error { err },
        };
        Event {
            node: self.node,
            remote: self.remote,
            conn: self.conn,
            event,
        }
    }

    pub fn remote(&self) -> &N {
        &self.remote
    }
}

#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    derive_more::Display,
    derive_more::Debug,
    derive_more::Deref,
    derive_more::From,
)]
#[display("n{_0}")]
#[debug("n{_0}")]
pub struct Nid(usize);

impl polestar::id::Id for Nid {}

#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    derive_more::Display,
    derive_more::Debug,
    derive_more::Deref,
    derive_more::From,
)]
#[display("c{_0}")]
#[debug("c{_0}")]
pub struct Cid(usize);
impl polestar::id::Id for Cid {}

#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    derive_more::Display,
    derive_more::Debug,
    derive_more::Deref,
    derive_more::From,
)]
#[display("s{_0}")]
#[debug("s{_0}")]
pub struct Sid(usize);
impl polestar::id::Id for Sid {}

pub type EventMappingShared = Option<Arc<Mutex<EventMapping>>>;

#[derive(Default)]
pub struct EventMapping {
    pub nodes: IdMap<NodeId, Nid>,
    pub conns: IdMap<u64, Cid>,
    pub streams: IdMap<StreamId, Sid>,
}

impl EventMapping {
    pub fn apply(&mut self, event: EventSystem) -> EventModel {
        assert_ne!(event.node, *event.remote(), "{event:?}");
        event
            .map_nodes(|node| self.nodes.lookup(node).unwrap())
            .map_conns(|conn| self.conns.lookup(conn).unwrap())
            .map_streams(|stream| self.streams.lookup(stream).unwrap())
    }

    pub(crate) fn print_info(&mut self, node_id: NodeId, conns: &Connections) {
        let node_id = self.nodes.lookup(node_id).unwrap();

        let initiated = conns
            .initiated
            .iter()
            .map(|((k, _), _)| self.nodes.lookup(*k).unwrap())
            .collect::<Vec<_>>();

        let accepted = conns
            .accepted
            .inner
            .iter()
            .flat_map(|((k, _), v)| repeat(self.nodes.lookup(*k).unwrap()).take(v.len()))
            .collect::<Vec<_>>();

        println!(
            "CONNECTIONS {node_id} : initiated={:?}, accepted={:?}",
            initiated, accepted
        );
    }
}

pub fn emit_event(
    event: EventSystem,
    node_id: NodeId,
    lock: &mut EventMapping,
    conns: Option<&Connections>,
) {
    let event = lock.apply(event);
    println!("{}", event);

    if let Some(conns) = conns {
        println!();
        lock.print_info(node_id, conns);
        println!();
    }
}

use std::{iter::repeat, sync::Arc};

use iroh::{NodeId, endpoint::StreamId};
use polestar::id::IdMap;
use tokio::sync::Mutex;

use crate::basic::Connections;

#[derive(Debug)]
pub struct Event<N, C, S> {
    node: N,
    event: EventType<N, C, S>,
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
        match &self.event {
            EventType::OpenConnection { to, conn } => {
                write!(f, "{:>16} : {node} -> {to} : {conn}", "OpenConnection")
            }
            EventType::AcceptConnection { from, conn } => {
                write!(f, "{:>16} : {node} <- {from} : {conn}", "AcceptConnection")
            }
            EventType::CloseConnection { to, conn } => {
                write!(f, "{:>16} : {node} xx {to} : {conn}", "CloseConnection")
            }
            EventType::OpenStream { to, conn, stream } => {
                write!(
                    f,
                    "{:>16} : {node} ~> {to} : {conn} . {stream}",
                    "OpenStream"
                )
            }
            EventType::AcceptStream { from, conn, stream } => {
                write!(
                    f,
                    "{:>16} : {node} <~ {from} : {conn} . {stream}",
                    "AcceptStream"
                )
            }
            EventType::EndStream { to, conn, stream } => {
                write!(
                    f,
                    "{:>16} : {node} xx {to} : {conn} . {stream}",
                    "EndStream"
                )
            }
            EventType::Error { to, conn, err } => {
                write!(f, "{:>16} : {node} XX {to} : {conn} : {err}", "Error")
            }
        }
    }
}
impl<N, C, S> Event<N, C, S> {
    pub fn new(node: impl Into<N>, event: EventType<impl Into<N>, C, S>) -> Self {
        let node = node.into();
        let event = event.map_nodes(Into::into);
        Self { node, event }
    }
}

pub type EventTypeSystem = EventType<NodeId, u64, iroh::endpoint::StreamId>;

#[derive(Debug)]
pub enum EventType<N, C, S> {
    OpenConnection {
        to: N,
        conn: C,
    },
    AcceptConnection {
        from: N,
        conn: C,
    },
    CloseConnection {
        to: N,
        conn: C,
    },
    OpenStream {
        to: N,
        conn: C,
        stream: S,
    },
    AcceptStream {
        from: N,
        conn: C,
        stream: S,
    },
    EndStream {
        to: N,
        conn: C,
        stream: S,
    },

    Error {
        to: N,
        conn: C,
        // stream: Option<S>,
        err: String,
    },
}

impl<N, C, S> EventType<N, C, S> {
    pub fn map_nodes<X>(self, mut f: impl FnMut(N) -> X) -> EventType<X, C, S> {
        match self {
            EventType::OpenConnection { to, conn } => EventType::OpenConnection { to: f(to), conn },
            EventType::AcceptConnection { from, conn } => EventType::AcceptConnection {
                from: f(from),
                conn,
            },
            EventType::CloseConnection { to, conn } => {
                EventType::CloseConnection { to: f(to), conn }
            }
            EventType::OpenStream { to, conn, stream } => EventType::OpenStream {
                to: f(to),
                conn,
                stream,
            },
            EventType::AcceptStream { from, conn, stream } => EventType::AcceptStream {
                from: f(from),
                conn,
                stream,
            },
            EventType::EndStream { to, conn, stream } => EventType::EndStream {
                to: f(to),
                conn,
                stream,
            },
            EventType::Error { to, conn, err } => EventType::Error {
                to: f(to),
                conn,
                err,
            },
        }
    }

    pub fn map_conns<X>(self, mut f: impl FnMut(C) -> X) -> EventType<N, X, S> {
        match self {
            EventType::OpenConnection { to, conn } => {
                EventType::OpenConnection { to, conn: f(conn) }
            }
            EventType::AcceptConnection { from, conn } => EventType::AcceptConnection {
                from,
                conn: f(conn),
            },
            EventType::CloseConnection { to, conn } => {
                EventType::CloseConnection { to, conn: f(conn) }
            }
            EventType::OpenStream { to, conn, stream } => EventType::OpenStream {
                to,
                conn: f(conn),
                stream,
            },
            EventType::AcceptStream { from, conn, stream } => EventType::AcceptStream {
                from,
                conn: f(conn),
                stream,
            },
            EventType::EndStream { to, conn, stream } => EventType::EndStream {
                to,
                conn: f(conn),
                stream,
            },
            EventType::Error { to, conn, err } => EventType::Error {
                to,
                conn: f(conn),
                err,
            },
        }
    }

    pub fn map_streams<X>(self, mut f: impl FnMut(S) -> X) -> EventType<N, C, X> {
        match self {
            EventType::OpenConnection { to, conn } => EventType::OpenConnection { to, conn },
            EventType::AcceptConnection { from, conn } => {
                EventType::AcceptConnection { from, conn }
            }
            EventType::CloseConnection { to, conn } => EventType::CloseConnection { to, conn },
            EventType::OpenStream { to, conn, stream } => EventType::OpenStream {
                to,
                conn,
                stream: f(stream),
            },
            EventType::AcceptStream { from, conn, stream } => EventType::AcceptStream {
                from,
                conn,
                stream: f(stream),
            },
            EventType::EndStream { to, conn, stream } => EventType::EndStream {
                to,
                conn,
                stream: f(stream),
            },
            EventType::Error { to, conn, err } => EventType::Error { to, conn, err },
        }
    }

    pub fn remote(&self) -> &N {
        match self {
            EventType::OpenConnection { to, .. } => to,
            EventType::AcceptConnection { from, .. } => from,
            EventType::CloseConnection { to, .. } => to,
            EventType::OpenStream { to, .. } => to,
            EventType::AcceptStream { from, .. } => from,
            EventType::EndStream { to, .. } => to,
            EventType::Error { to, .. } => to,
        }
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
        assert_ne!(event.node, *event.event.remote(), "{event:?}");
        Event {
            node: self.nodes.lookup(event.node).unwrap(),
            event: event
                .event
                .map_nodes(|node| self.nodes.lookup(node).unwrap())
                .map_conns(|conn| self.conns.lookup(conn).unwrap())
                .map_streams(|stream| self.streams.lookup(stream).unwrap()),
        }
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

    return;

    if let Some(conns) = conns {
        println!();
        lock.print_info(node_id, conns);
        println!();
    }
}

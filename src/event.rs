use iroh::NodeId;

#[derive(Debug, derive_more::Display)]
#[display("{node}: {action}")]
pub struct Event<N> {
    node: N,
    action: Action<N>,
}

impl<N> Event<N> {
    pub fn new(node: impl Into<N>, action: Action<impl Into<N>>) -> Self {
        let node = node.into();
        let action = action.map(Into::into);
        Self { node, action }
    }
}

#[derive(Debug, derive_more::Display)]
pub enum Action<N> {
    #[display("OpenConnection({to})")]
    OpenConnection { to: N },
    #[display("CloseConnection({to})")]
    CloseConnection { to: N },
    #[display("AcceptConnection({from})")]
    AcceptConnection { from: N },
}

impl<N> Action<N> {
    pub fn map<M>(self, f: impl Fn(N) -> M) -> Action<M> {
        match self {
            Action::OpenConnection { to } => Action::OpenConnection { to: f(to) },
            Action::CloseConnection { to } => Action::CloseConnection { to: f(to) },
            Action::AcceptConnection { from } => Action::AcceptConnection { from: f(from) },
        }
    }
}

#[derive(Debug, derive_more::Deref, derive_more::From)]
pub struct Nid(NodeId);

impl std::fmt::Display for Nid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.fmt_short())
    }
}

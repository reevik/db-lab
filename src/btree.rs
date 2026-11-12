const FAN_OUT: u8 = 1 << 3;

struct Index {
    root: Option<Box<Node>>,
}

impl Index {
}

struct Node {
    // child page IDs.
    children: Vec<usize>,
    // Scope of the children, the keys.
    keys: Vec<usize>,
    // Node type can be either an inner or a leaf node.
    node_type: NodeType,
}

enum NodeType {
    Leaf,
    Inner,
}

impl Node {}

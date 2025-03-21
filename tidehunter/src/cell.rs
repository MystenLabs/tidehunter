use minibytes::Bytes;

#[derive(Debug, Eq, PartialEq)]
pub enum CellId {
    Integer(usize),
    Bytes(Bytes),
}

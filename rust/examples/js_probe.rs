const HEX: &str = "856f4a830373a66001590010c8a6cd480f4a4ee3a08b6c61cfcb7b2c0101000000061515340142025605571670027d057469746c6505636f756e7407656e61626c65640303017dd602140248656c6c6f2066726f6d204a6176615363726970742a0300";
fn main() {
    let raw = hex::decode(HEX).unwrap();
    let change = ::automerge::Change::from_bytes(raw).unwrap();
    println!("stored change timestamp (raw i64): {}", change.timestamp());
    println!("message: {:?}", change.message());
}

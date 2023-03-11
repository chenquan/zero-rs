#![feature(type_alias_impl_trait)]
#![feature(duration_constants)]

mod discover;
mod loadbalance;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use num_integer::Roots;
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        let x =22;
        let i = x.sqrt();
        assert_eq!(result, 4);
    }
}

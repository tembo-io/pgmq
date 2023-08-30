use crate::errors::PgmqExtError;

pub fn delay_to_u64(delay: i32) -> Result<u64, PgmqExtError> {
    if delay >= 0 {
        Ok(delay as u64)
    } else {
        Err(PgmqExtError::InvalidDelay(delay))
    }
}

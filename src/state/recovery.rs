use crate::error::AppResult;
use crate::state::store::StateStore;

pub fn recover(_store: &StateStore) -> AppResult<Vec<String>> {
    Ok(vec!["no recovery actions implemented yet".to_string()])
}

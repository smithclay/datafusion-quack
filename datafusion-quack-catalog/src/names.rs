//! DuckDB name resolution: catalogs, schemas and tables are found regardless of case.
//!
//! DuckDB matches names case-insensitively, quoted or not, so a client may ask for
//! `"mixedcase"` when the table is `MixedCase`. These wrappers look a name up
//! exactly first, then case-insensitively when exactly one entry matches. They also
//! give every catalog an `information_schema` with DuckDB type names, in place of any
//! the catalog has (see [`crate::information_schema`]).

use std::sync::{Arc, Weak};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};
use datafusion::error::Result;

use crate::information_schema::{DuckDbInformationSchema, INFORMATION_SCHEMA};

/// Finds `name` in `names`: exactly, or else case-insensitively if only one matches.
pub(crate) fn resolve<'a>(
    name: &str,
    names: impl IntoIterator<Item = &'a String>,
) -> Option<String> {
    let mut folded = None;
    for candidate in names {
        if candidate == name {
            return Some(candidate.clone());
        }
        if candidate.eq_ignore_ascii_case(name) {
            if folded.is_some() {
                // ambiguous: `T` and `t` both exist, and neither is an exact match
                return None;
            }
            folded = Some(candidate.clone());
        }
    }
    folded
}

/// A catalog list with DuckDB name resolution.
#[derive(Debug)]
pub struct DuckDbCatalogList {
    inner: Arc<dyn CatalogProviderList>,
    this: Weak<DuckDbCatalogList>,
}

impl DuckDbCatalogList {
    /// Wraps `inner`. A list that is already wrapped is returned as it is.
    pub fn wrap(inner: Arc<dyn CatalogProviderList>) -> Arc<dyn CatalogProviderList> {
        if inner.is::<DuckDbCatalogList>() {
            return inner;
        }
        Arc::new_cyclic(|this| Self {
            inner,
            this: this.clone(),
        })
    }

    /// The wrapped list.
    pub fn inner(&self) -> &Arc<dyn CatalogProviderList> {
        &self.inner
    }
}

impl CatalogProviderList for DuckDbCatalogList {
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.inner.register_catalog(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.inner.catalog_names()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        let inner = match self.inner.catalog(name) {
            Some(inner) => inner,
            None => self
                .inner
                .catalog(&resolve(name, &self.inner.catalog_names())?)?,
        };
        let list: Arc<dyn CatalogProviderList> = self.this.upgrade()?;
        Some(Arc::new(DuckDbCatalog { inner, list }))
    }
}

/// A catalog with DuckDB name resolution and a DuckDB `information_schema`.
#[derive(Debug)]
pub struct DuckDbCatalog {
    inner: Arc<dyn CatalogProvider>,
    list: Arc<dyn CatalogProviderList>,
}

impl CatalogProvider for DuckDbCatalog {
    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // DuckDB's, even over a catalog's own: DuckDB clients read its columns
        if name.eq_ignore_ascii_case(INFORMATION_SCHEMA) {
            return Some(Arc::new(DuckDbInformationSchema::new(Arc::clone(
                &self.list,
            ))));
        }
        let inner = self.inner.schema(name).or_else(|| {
            self.inner
                .schema(&resolve(name, &self.inner.schema_names())?)
        })?;
        Some(Arc::new(DuckDbSchema { inner }))
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        self.inner.register_schema(name, schema)
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        let name = resolve(name, &self.inner.schema_names()).unwrap_or_else(|| name.to_string());
        self.inner.deregister_schema(&name, cascade)
    }
}

/// A schema with case-insensitive table names.
#[derive(Debug)]
pub struct DuckDbSchema {
    inner: Arc<dyn SchemaProvider>,
}

impl DuckDbSchema {
    fn resolve(&self, name: &str) -> String {
        if self.inner.table_exist(name) {
            return name.to_string();
        }
        resolve(name, &self.inner.table_names()).unwrap_or_else(|| name.to_string())
    }
}

#[async_trait]
impl SchemaProvider for DuckDbSchema {
    fn owner_name(&self) -> Option<&str> {
        self.inner.owner_name()
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.table(&self.resolve(name)).await
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(&self.resolve(name))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name) || resolve(name, &self.inner.table_names()).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn exact_wins_then_a_unique_case_insensitive_match() {
        let n = names(&["MixedCase", "t", "T", "other"]);
        assert_eq!(resolve("MixedCase", &n).as_deref(), Some("MixedCase"));
        assert_eq!(resolve("mixedcase", &n).as_deref(), Some("MixedCase"));
        assert_eq!(resolve("t", &n).as_deref(), Some("t"));
        assert_eq!(resolve("T", &n).as_deref(), Some("T"));
        assert_eq!(resolve("OTHER", &n).as_deref(), Some("other"));
        assert_eq!(resolve("missing", &n), None);
        // ambiguous without an exact match
        assert_eq!(resolve("x", &names(&["X", "x "])).as_deref(), Some("X"));
        assert_eq!(resolve("ab", &names(&["AB", "Ab"])), None);
    }
}

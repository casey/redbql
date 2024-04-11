use redb::{ReadTransaction, WriteTransaction};

pub trait Query<'a, T = ()> {
  type Output;
  type Error: From<redb::Error>;

  fn run(self, tx: &'a ReadTransaction) -> Result<Self::Output, Self::Error>;
}

pub trait Statement<'a, T> {
  type Output;
  type Error: From<redb::Error>;

  fn execute(self, tx: &'a WriteTransaction) -> Result<Self::Output, Self::Error>;
}

trait StatementArg<'a>: Sized {
  fn from_tx(tx: &'a WriteTransaction) -> Result<Self, redb::Error>;
}

impl<'a> StatementArg<'a> for &'a WriteTransaction {
  fn from_tx(tx: &'a WriteTransaction) -> Result<Self, redb::Error> {
    Ok(tx)
  }
}

trait QueryArg<'a>: Sized {
  fn from_tx(tx: &'a ReadTransaction) -> Result<Self, redb::Error>;
}

impl<'a> QueryArg<'a> for &'a ReadTransaction {
  fn from_tx(tx: &'a ReadTransaction) -> Result<Self, redb::Error> {
    Ok(tx)
  }
}

impl<'a, F, O, E, T0> Query<'a, (T0,)> for F
where
  F: FnOnce(T0) -> Result<O, E>,
  T0: QueryArg<'a>,
  E: From<redb::Error>,
{
  type Output = O;
  type Error = E;

  fn run(self, tx: &'a ReadTransaction) -> Result<Self::Output, Self::Error> {
    let t0 = T0::from_tx(tx)?;
    self(t0)
  }
}

impl<'a, F, O, E, T0, T1> Query<'a, (T0, T1)> for F
where
  F: FnOnce(T0, T1) -> Result<O, E>,
  T0: QueryArg<'a>,
  T1: QueryArg<'a>,
  E: From<redb::Error>,
{
  type Output = O;
  type Error = E;

  fn run(self, tx: &'a ReadTransaction) -> Result<Self::Output, Self::Error> {
    let t0 = T0::from_tx(tx)?;
    let t1 = T1::from_tx(tx)?;
    self(t0, t1)
  }
}

impl<'a, F, O, E, T0> Statement<'a, (T0,)> for F
where
  F: FnOnce(T0) -> Result<O, E>,
  T0: StatementArg<'a>,
  E: From<redb::Error>,
{
  type Output = O;
  type Error = E;

  fn execute(self, tx: &'a WriteTransaction) -> Result<Self::Output, Self::Error> {
    let t0 = T0::from_tx(tx)?;
    self(t0)
  }
}

impl<'a, F, O, E, T0, T1> Statement<'a, (T0, T1)> for F
where
  F: FnOnce(T0, T1) -> Result<O, E>,
  T0: StatementArg<'a>,
  T1: StatementArg<'a>,
  E: From<redb::Error>,
{
  type Output = O;
  type Error = E;

  fn execute(self, tx: &'a WriteTransaction) -> Result<Self::Output, Self::Error> {
    let t0 = T0::from_tx(tx)?;
    let t1 = T1::from_tx(tx)?;
    self(t0, t1)
  }
}

#[macro_export]
macro_rules! table {
  ($ro:ident, $rw:ident, $name:ident, $key:ty, $value:ty) => {
    struct $rw<'a>(::redb::Table<'a, $key, $value>);

    const $name: ::redb::TableDefinition<'static, $key, $value> =
      ::redb::TableDefinition::new(stringify!($name));

    impl<'a> StatementArg<'a> for $rw<'a> {
      fn from_tx(tx: &'a ::redb::WriteTransaction) -> Result<Self, ::redb::Error> {
        Ok(Self(tx.open_table($name)?))
      }
    }

    struct $ro(::redb::ReadOnlyTable<$key, $value>);

    impl<'a> QueryArg<'a> for $ro {
      fn from_tx(tx: &'a ::redb::ReadTransaction) -> Result<Self, ::redb::Error> {
        Ok(Self(tx.open_table($name)?))
      }
    }
  };
}

// This is commented out, because I can't get it to compile T_T
#[cfg(any())]
mod ext {
  use {super::*, redb::Database};

  trait DatabaseExt {
    fn execute<'a, T, S>(&self, statement: S) -> Result<S::Output, S::Error>
    where
      S: Statement<'a, T>;
  }

  impl DatabaseExt for Database {
    fn execute<'a, T, S>(&self, statement: S) -> Result<S::Output, S::Error>
    where
      S: Statement<'a, T>,
    {
      let tx = self.begin_write().map_err(|err| redb::Error::from(err))?;
      let result = statement.execute(&tx)?;
      tx.commit().map_err(|err| redb::Error::from(err))?;
      Ok(result)
    }
  }
}

#[cfg(test)]
mod tests {
  use {super::*, redb::Database, tempfile::TempDir};

  table! {
    Names, NamesMut, NAMES, &'static str, &'static str
  }

  fn initialize(mut names: NamesMut) -> Result<(), redb::Error> {
    names.0.insert("james", "smith")?;
    Ok(())
  }

  fn create() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();

    let database = Database::create(dir.path().join("database.redb")).unwrap();

    let tx = database.begin_write().unwrap();

    initialize.execute(&tx).unwrap();

    tx.commit().unwrap();

    (dir, database)
  }

  #[test]
  fn function() {
    fn get(names: Names) -> Result<Option<String>, redb::Error> {
      Ok(names.0.get("james")?.map(|guard| guard.value().into()))
    }

    let (_dir, database) = create();

    let tx = database.begin_read().unwrap();

    let result = get.run(&tx).unwrap();

    assert_eq!(result, Some("smith".into()));
  }

  #[test]
  fn closure() {
    let (_dir, database) = create();

    let tx = database.begin_read().unwrap();

    let name = "james";

    let query = |names: Names| -> Result<Option<String>, redb::Error> {
      Ok(names.0.get(name)?.map(|guard| guard.value().into()))
    };

    let result = query.run(&tx).unwrap();

    assert_eq!(result, Some("smith".into()));
  }

  #[test]
  fn object() {
    struct Foo {
      name: String,
    }

    impl<'a> Query<'a> for Foo {
      type Output = Option<String>;
      type Error = redb::Error;

      fn run(self, tx: &'a ReadTransaction) -> Result<Self::Output, Self::Error> {
        let closure = |names: Names| -> Result<Option<String>, redb::Error> {
          Ok(names.0.get(&*self.name)?.map(|guard| guard.value().into()))
        };
        closure.run(tx)
      }
    }

    let (_dir, database) = create();

    let tx = database.begin_read().unwrap();

    let result = Foo {
      name: "james".into(),
    }
    .run(&tx)
    .unwrap();

    assert_eq!(result, Some("smith".into()));
  }
}

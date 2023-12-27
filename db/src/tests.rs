use chrono::DateTime;
use crate::*;
use tempfile::tempdir;
use crate::database::SavedDatabase;
use chrono::prelude::*;

#[test]
fn save_load() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    std::fs::File::create(&path).unwrap();
    let db = SavedDatabase::create("db".to_string(), path.to_str().unwrap().to_string()).unwrap();

    let db = SavedDatabase::load_from_disk(path.to_str().unwrap().to_string()).unwrap();
    assert_eq!(db.get_name(), "db");
}

#[test]
fn test_table_crud() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    std::fs::File::create(&path).unwrap();
    let mut db =
        SavedDatabase::create("db".to_string(), path.to_str().unwrap().to_string()).unwrap();

    db.create_table("table".to_string(), vec![DbType::Int])
        .unwrap();
    {
        let table = db.get_table_mut("table".to_string()).unwrap();

        table.insert_row(Row(vec![DbValue::Int(1)])).unwrap();
        assert_eq!(table.rows().len(), 1);

        table.update_row(0, Row(vec![DbValue::Int(2)])).unwrap();
        assert_eq!(table.rows().len(), 1);
        assert_eq!(table.rows()[0], Row(vec![DbValue::Int(2)]));

        table.remove_row(0);
        assert_eq!(table.rows().len(), 0);
    }

    db.remove_table("table".to_string()).unwrap();
    assert!(db.get_table_names().is_empty());
}

#[test]
fn table_projection() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    std::fs::File::create(&path).unwrap();
    let mut db =
        SavedDatabase::create("db".to_string(), path.to_str().unwrap().to_string()).unwrap();

    db.create_table(
        "table".to_string(),
        vec![DbType::String, DbType::Time],
    )
    .unwrap();
    let table = db.get_table_mut("table".to_string()).unwrap();

    let row1 = Row(vec![
        DbValue::String("B".to_string()),
        DbValue::Time(DateTime::default()),
    ]);
    let row2 = Row(vec![
        DbValue::String("C".to_string()),
        DbValue::Time(DateTime::<Utc>::from_utc(NaiveDate::from_ymd(2016, 7, 8).and_hms(9, 10, 11), Utc)),
    ]);
    table.insert_row(row1.clone()).unwrap();
    table.insert_row(row2.clone()).unwrap();

    db.projection("table".to_string(), vec![true, false], "projection".to_string()).unwrap();

    let projection_table = db.get_table("projection".to_string()).unwrap();
    assert_eq!(projection_table.schema(), vec![DbType::String]);
    let mut iter = projection_table.rows().iter();
    assert_eq!(iter.next().unwrap().clone(), Row(vec![DbValue::String("B".to_string())]));
    assert_eq!(iter.next().unwrap().clone(), Row(vec![DbValue::String("C".to_string())]));
    assert_eq!(iter.next(), None);
}

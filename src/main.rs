use csv;
use imessage_database::message_types::variants::{Reaction, Variant};
use imessage_database::tables::handle::Handle;
use imessage_database::util::dates::get_offset;
use imessage_database::{
    tables::{
        chat::Chat,
        messages::Message,
        table::{get_connection, Table, CHAT_MESSAGE_JOIN, MESSAGE, MESSAGE_ATTACHMENT_JOIN},
    },
    util::dirs::default_db_path,
};
use rusqlite::{Connection, Statement};
use serde::Serialize;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::path::Path;
use std::process::exit;

const GM_ID: &str = "chat62090849848071634";

#[derive(Debug, Serialize)]
struct ReactionData {
    message_id: String,
    author: String,
    reaction_type: String,
}

#[derive(Debug, Serialize)]
struct MessageData {
    pub id: String,
    pub text: Option<String>,
    pub author: String,
    pub date: i64,
    pub associated_message_guid: Option<String>,
    pub thread_originator_guid: Option<String>,
}

fn clean_associated_guid(s: Option<String>) -> Option<String> {
    if let Some(guid) = s {
        if guid.starts_with("p:") {
            let mut split = guid.split('/');
            let index_str = split.next();
            let message_id = split.next();
            let _index = str::parse::<usize>(&index_str.unwrap().replace("p:", "")).unwrap_or(0);
            return Some(String::from(message_id.unwrap()));
        } else if guid.starts_with("bp:") {
            return Some(String::from(&guid[3..guid.len()]));
        } else {
            return Some(String::from(guid.as_str()));
        }
    }
    None
}

fn get_gm_query(db: &Connection, gm_id: i32) -> Statement {
    db.prepare(&format!(
        "SELECT
                 *,
                 c.chat_id,
                 (SELECT COUNT(*) FROM {MESSAGE_ATTACHMENT_JOIN} a WHERE m.ROWID = a.message_id) as num_attachments,
                 (SELECT COUNT(*) FROM {MESSAGE} m2 WHERE m2.thread_originator_guid = m.guid) as num_replies
             FROM
                 message as m
                 LEFT JOIN {CHAT_MESSAGE_JOIN} as c ON m.ROWID = c.message_id
             WHERE c.chat_id = {gm_id}
             ORDER BY
                 m.date;
            "
    ))
        .unwrap_or_else(|_| db.prepare(&format!(
            "SELECT
                 *,
                 c.chat_id,
                 (SELECT COUNT(*) FROM {MESSAGE_ATTACHMENT_JOIN} a WHERE m.ROWID = a.message_id) as num_attachments,
                 (SELECT 0) as num_replies
             FROM
                 message as m
                 LEFT JOIN {CHAT_MESSAGE_JOIN} as c ON m.ROWID = c.message_id
             WHERE c.chat_id = {gm_id}
             ORDER BY
                 m.date;
            "
        )).unwrap())
}

fn get_gm_data(db: &Connection, gm_id: i32) -> (Vec<MessageData>, Vec<ReactionData>) {
    let mut statement = get_gm_query(db, gm_id);

    let messages = statement
        .query_map([], |row| Ok(Message::from_row(row)))
        .unwrap();

    let mut handle = match Handle::get(&db) {
        Ok(h) => h,
        Err(e) => panic!("Handle failed with error {}", e.to_string()),
    };
    let handles = handle
        .query_map([], |row| Ok(Handle::from_row(row)))
        .unwrap();

    let mut authors = HashMap::new();
    for hand in handles {
        let hand = Handle::extract(hand);
        if let Ok(h) = hand {
            authors.insert(h.rowid, h.id.trim_start_matches('+').to_string());
        }
    }

    // Handles table doesnt include your own number, add mine here
    let args: Vec<String> = env::args().collect();
    authors.insert(0, String::from(&args[1]));

    let mut ret_messages = Vec::new();
    let mut ret_reactions = Vec::new();

    for message in messages {
        let mut msg = match Message::extract(message) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("ERROR: {}", e);
                continue;
            }
        };
        let author = match authors.get(&msg.handle_id) {
            None => {
                eprintln!("ERROR: Found invalid handle id {:?}", msg);
                continue;
            }
            Some(a) => a,
        }
        .to_string();

        if let Variant::Normal = msg.variant() {
            let _ = msg.gen_text(&db);
            let date = msg
                .date_delivered(&get_offset())
                .unwrap()
                .timestamp_millis();

            ret_messages.push(MessageData {
                id: msg.guid,
                text: msg.text,
                author,
                date,
                associated_message_guid: msg.associated_message_guid,
                thread_originator_guid: msg.thread_originator_guid,
            });
        } else if let Variant::Reaction(_, _, react_type) = msg.variant() {
            ret_reactions.push(ReactionData {
                message_id: clean_associated_guid(msg.associated_message_guid).unwrap(),
                author,
                reaction_type: String::from(match react_type {
                    Reaction::Loved => "Loved",
                    Reaction::Liked => "Liked",
                    Reaction::Disliked => "Disliked",
                    Reaction::Laughed => "Laughed",
                    Reaction::Emphasized => "Emphasized",
                    Reaction::Questioned => "Questioned",
                }),
            });
        }
    }
    return (ret_messages, ret_reactions);
}

fn get_gm_id(db: &Connection) -> Option<i32> {
    let mut chats = match Chat::get(&db) {
        Ok(c) => c,
        Err(e) => panic!("Failed to get chat db handle with error {}", e.to_string()),
    };

    let db_chats = chats.query_map([], |row| Ok(Chat::from_row(row))).unwrap();

    for chat in db_chats {
        let extract_chat = Chat::extract(chat);

        if let Ok(c) = extract_chat {
            if c.chat_identifier == GM_ID {
                return Some(c.rowid);
            }
        }
    }

    return None;
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("ERROR: No local number specified bailing out");
        exit(-1);
    }

    // Create a read-only connection to the iMessage database
    let db = get_connection(&default_db_path()).unwrap();

    let gm_id = match get_gm_id(&db) {
        Some(id) => {
            println!("GM RowId Found: {}", id);
            id
        }
        None => {
            eprintln!("ERROR: Could not find GM RowId");
            exit(1);
        }
    };

    let queried_data = get_gm_data(&db, gm_id);

    let path = Path::new("messages.csv");
    let display = path.display();
    let file = match File::create(&path) {
        Err(why) => panic!("couldn't create {}: {}", display, why),
        Ok(file) => file,
    };
    println!("Writing messages.csv at path {}", display);

    let mut wtr = csv::Writer::from_writer(file);

    for message in queried_data.0 {
        match wtr.serialize(message) {
            Ok(_) => (),
            Err(e) => eprintln!("Serialization of message failed with error {}", e),
        }
    }

    let path = Path::new("reactions.csv");
    let display = path.display();
    let file = match File::create(&path) {
        Err(why) => panic!("couldn't create {}: {}", display, why),
        Ok(file) => file,
    };
    println!("Writing reactions.csv at path {}", display);

    let mut wtr = csv::Writer::from_writer(file);

    for reaction in queried_data.1 {
        match wtr.serialize(reaction) {
            Ok(_) => (),
            Err(e) => eprintln!("Serialization of reaction failed with error {}", e),
        }
    }

    println!("Export Complete");
}

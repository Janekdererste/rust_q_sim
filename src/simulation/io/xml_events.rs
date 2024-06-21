use std::any::Any;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

use tracing::info;
use xml::attribute::OwnedAttribute;
use xml::reader::XmlEvent;
use xml::EventReader;

use crate::simulation::id::Id;
use crate::simulation::messaging::events::EventsSubscriber;
use crate::simulation::network::global_network::Link;
use crate::simulation::wire_types::events::event::Type;
use crate::simulation::wire_types::events::Event;
use crate::simulation::wire_types::messages::Vehicle;
use crate::simulation::wire_types::population::Person;

pub struct XmlEventsWriter {
    writer: BufWriter<File>,
}

impl XmlEventsWriter {
    pub fn new(path: &Path) -> Self {
        info!("Creating file: {path:?}");
        let file = File::create(path).expect("Failed to create File.");
        let mut writer = BufWriter::new(file);
        let header = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<events version=\"1.0\">\n";
        writer
            .write_all(header.as_bytes())
            .expect("Failed to write events file header");
        XmlEventsWriter { writer }
    }

    pub fn event_2_string(time: u32, event: &Event) -> String {
        match event.r#type.as_ref().unwrap() {
            Type::Generic(e) => {
                format!(
                    "<event time=\"{time}\" type=\"{}\" attrs is not yet implemented. Sorry/>\n",
                    e.r#type
                )
            }
            Type::ActStart(e) => {
                format!("<event time=\"{time}\" type=\"actstart\" person=\"{}\" link=\"{}\" actType=\"{}\" />\n",
                        Id::<Person>::get(e.person).external(),
                        Id::<Link>::get(e.link).external(),
                        Id::<String>::get(e.act_type).external())
            }
            Type::ActEnd(e) => {
                format!("<event time=\"{time}\" type=\"actend\" person=\"{}\" link=\"{}\" actType=\"{}\" />\n",
                        Id::<Person>::get(e.person).external(),
                        Id::<Link>::get(e.link).external(),
                        Id::<String>::get(e.act_type).external())
            }
            Type::LinkEnter(e) => {
                format!(
                    "<event time=\"{time}\" type=\"entered link\" link=\"{}\" vehicle=\"{}\" />\n",
                    Id::<Link>::get(e.link).external(),
                    Id::<Vehicle>::get(e.vehicle).external()
                )
            }
            Type::LinkLeave(e) => {
                format!(
                    "<event time=\"{time}\" type=\"left link\" link=\"{}\" vehicle=\"{}\" />\n",
                    Id::<Link>::get(e.link).external(),
                    Id::<Vehicle>::get(e.vehicle).external()
                )
            }
            Type::PersonEntersVeh(e) => {
                format!("<event time=\"{time}\" type=\"PersonEntersVehicle\" person=\"{}\" vehicle=\"{}\" />\n",
                        Id::<Person>::get(e.person).external(), Id::<Vehicle>::get(e.vehicle).external())
            }
            Type::PersonLeavesVeh(e) => {
                format!("<event time=\"{time}\" type=\"PersonLeavesVehicle\" person=\"{}\" vehicle=\"{}\" />\n",
                        Id::<Person>::get(e.person).external(), Id::<Vehicle>::get(e.vehicle).external())
            }
            Type::Departure(e) => {
                format!("<event time=\"{time}\" type=\"departure\" person=\"{}\" link=\"{}\" legMode=\"{}\" />\n",
                        Id::<Person>::get(e.person).external(),
                        Id::<Link>::get(e.link).external(),
                        Id::<String>::get(e.leg_mode).external())
            }
            Type::Arrival(e) => {
                format!("<event time=\"{time}\" type=\"arrival\" person=\"{}\" link=\"{}\" legMode=\"{}\" />\n",
                        Id::<Person>::get(e.person).external(),
                        Id::<Link>::get(e.link).external(),
                        Id::<String>::get(e.leg_mode).external())
            }
            Type::Travelled(e) => {
                format!("<event time=\"{time}\" type=\"travelled\" person=\"{}\" distance=\"{}\" mode=\"{}\" />\n",
                        Id::<Person>::get(e.person).external(),
                        e.distance,
                        Id::<String>::get(e.mode).external())
            }
            Type::PassengerPickedUp(e) => {
                format!("<event time=\"{time}\" type=\"passenger picked up\" person=\"{}\" mode=\"{}\" request=\"{}\" vehicle=\"{}\"/>\n",
                        Id::<Person>::get(e.person).external(),
                        Id::<String>::get(e.mode).external(),
                        Id::<String>::get(e.request).external(),
                        Id::<Vehicle>::get(e.vehicle).external())
            }
            Type::PassengerDroppedOff(e) => {
                format!("<event time=\"{time}\" type=\"passenger dropped off\" person=\"{}\" mode=\"{}\" request=\"{}\" vehicle=\"{}\"/>\n",
                        Id::<Person>::get(e.person).external(),
                        Id::<String>::get(e.mode).external(),
                        Id::<String>::get(e.request).external(),
                        Id::<Vehicle>::get(e.vehicle).external())
            }
        }
    }

    fn write(&mut self, text: &str) {
        self.writer
            .write_all(text.as_bytes())
            .expect("Error while writing event");
    }
}

impl EventsSubscriber for XmlEventsWriter {
    fn receive_event(&mut self, time: u32, event: &Event) {
        let text = XmlEventsWriter::event_2_string(time, event);
        self.write(&text);
    }

    fn finish(&mut self) {
        let closing_tag = "</events>";
        self.write(closing_tag);
        info!("Finishing Events File. Calling flush on Buffered Writer.");
        self.writer.flush().expect("Failed to flush events.");
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct XmlEventsReader {
    parser: EventReader<BufReader<File>>,
}

impl XmlEventsReader {
    pub fn new(events_file: &Path) -> Self {
        let file = File::open(events_file)
            .unwrap_or_else(|_| panic!("Could not open events file: {:?}", events_file));
        let buffered_reader = BufReader::new(file);
        let parser = EventReader::new(buffered_reader);
        Self { parser }
    }
    pub fn read_next(&mut self) -> Option<(u32, Event)> {
        loop {
            let result = self.parser.next();
            match result {
                Ok(XmlEvent::StartElement {
                    name, attributes, ..
                }) => {
                    if name.local_name.eq("event") {
                        let time: u32 = attributes.get(0).unwrap().value.parse().unwrap();
                        let event = handle(attributes);
                        return Some((time, event));
                    }
                }
                Ok(XmlEvent::EndDocument) => return None,
                Err(_) => return None,
                _ => {
                    continue;
                }
            }
        }
    }
}

fn handle(attr: Vec<OwnedAttribute>) -> Event {
    let ev_type = &attr.get(1).unwrap().value;
    match ev_type.as_str() {
        "actend" => handle_act_end(attr),
        "departure" => handle_departure(attr),
        "travelled" => travelled(attr),
        "arrival" => handle_arrival(attr),
        "actstart" => handle_act_start(attr),
        "PersonEntersVehicle" => handle_person_enters_veh(attr),
        "PersonLeavesVehicle" => handle_person_leaves_veh(attr),
        "entered link" => handle_link_enter(attr),
        "left link" => handle_link_leave(attr),
        _ => panic!("Unknown event type {ev_type}"),
    }
}

fn handle_act_end(attr: Vec<OwnedAttribute>) -> Event {
    let person: Id<Person> = Id::create(&attr.get(2).unwrap().value);
    let link: Id<Link> = Id::create(&attr.get(3).unwrap().value);
    let act_type: Id<String> = Id::create(&attr.get(4).unwrap().value);
    Event::new_act_end(person.internal(), link.internal(), act_type.internal())
}

fn handle_act_start(attr: Vec<OwnedAttribute>) -> Event {
    let person: Id<Person> = Id::create(&attr.get(2).unwrap().value);
    let link: Id<Link> = Id::create(&attr.get(3).unwrap().value);
    let act_type: Id<String> = Id::create(&attr.get(4).unwrap().value);
    Event::new_act_start(person.internal(), link.internal(), act_type.internal())
}

fn handle_departure(attr: Vec<OwnedAttribute>) -> Event {
    let person: Id<Person> = Id::create(&attr.get(2).unwrap().value);
    let link: Id<Link> = Id::create(&attr.get(3).unwrap().value);
    let mode: Id<String> = Id::create(&attr.get(4).unwrap().value);
    Event::new_departure(person.internal(), link.internal(), mode.internal())
}

fn handle_arrival(attr: Vec<OwnedAttribute>) -> Event {
    let person: Id<Person> = Id::create(&attr.get(2).unwrap().value);
    let link: Id<Link> = Id::create(&attr.get(3).unwrap().value);
    let mode: Id<String> = Id::create(&attr.get(4).unwrap().value);
    Event::new_arrival(person.internal(), link.internal(), mode.internal())
}

fn travelled(attr: Vec<OwnedAttribute>) -> Event {
    let person: Id<Person> = Id::create(&attr.get(2).unwrap().value);
    let dist: f64 = attr.get(3).unwrap().value.parse().unwrap();
    let mode: Id<String> = Id::create(&attr.get(4).unwrap().value);
    Event::new_travelled(person.internal(), dist, mode.internal())
}

fn handle_person_enters_veh(attr: Vec<OwnedAttribute>) -> Event {
    let person: Id<Person> = Id::create(&attr.get(2).unwrap().value);
    let vehicle: Id<Vehicle> = Id::create(&attr.get(3).unwrap().value);
    Event::new_person_enters_veh(person.internal(), vehicle.internal())
}

fn handle_person_leaves_veh(attr: Vec<OwnedAttribute>) -> Event {
    let person: Id<Person> = Id::create(&attr.get(2).unwrap().value);
    let vehicle: Id<Vehicle> = Id::create(&attr.get(3).unwrap().value);
    Event::new_person_leaves_veh(person.internal(), vehicle.internal())
}

fn handle_link_enter(attr: Vec<OwnedAttribute>) -> Event {
    let link: Id<Link> = Id::create(&attr.get(2).unwrap().value);
    let vehicle: Id<Vehicle> = Id::create(&attr.get(3).unwrap().value);
    Event::new_link_enter(link.internal(), vehicle.internal())
}

fn handle_link_leave(attr: Vec<OwnedAttribute>) -> Event {
    let link: Id<Link> = Id::create(&attr.get(2).unwrap().value);
    let vehicle: Id<Vehicle> = Id::create(&attr.get(3).unwrap().value);
    Event::new_link_leave(link.internal(), vehicle.internal())
}

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;

use tracing::{info, instrument};

use crate::simulation::wire_types::events::event::Type::{
    ActEnd, ActStart, Arrival, Departure, Generic, LinkEnter, LinkLeave, PersonEntersVeh,
    PersonLeavesVeh, Travelled,
};
use crate::simulation::wire_types::events::{
    ActivityEndEvent, ActivityStartEvent, ArrivalEvent, DepartureEvent, Event, GenericEvent,
    LinkEnterEvent, LinkLeaveEvent, PersonEntersVehicleEvent, PersonLeavesVehicleEvent,
    TravelledEvent,
};

pub trait EventsSubscriber {
    fn receive_event(&mut self, time: u32, event: &Event);

    fn finish(&mut self) {}

    fn as_any(&mut self) -> &mut dyn Any;
}

impl Debug for dyn EventsSubscriber + Send {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EventsSubscriber")
    }
}

pub struct EventsLogger {}

impl EventsSubscriber for EventsLogger {
    fn receive_event(&mut self, time: u32, event: &Event) {
        info!("{time}: {event:?}");
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
}

#[derive(Default, Debug)]
pub struct EventsPublisher {
    handlers: Vec<Box<dyn EventsSubscriber + Send>>,
}

/// Events takes a writer. This is the trait for that
pub trait EventsWriter: EventsWriterClone + Send {
    fn write(&self, buf: Vec<u8>);
}

/// Since we want Events to be clonable but we also want the writer to be a dynamic object, we need to
/// specify how to clone the Box<dyn EventsWriter> property in Events. The following three method are
/// concerned about that. Implemented according to https://stackoverflow.com/questions/50017987/cant-clone-vecboxtrait-because-trait-cannot-be-made-into-an-object
///
pub trait EventsWriterClone {
    fn clone_boxed(&self) -> Box<dyn EventsWriter>;
}

/// this implements clone_boxed for all owned EventsWriters e.g. Box<dyn EventsWriter>. The 'static
/// lifetime applies to all values that are owned. See last paragraph (Trait bound) of
/// https://doc.rust-lang.org/rust-by-example/scope/lifetime/static_lifetime.html
impl<T: EventsWriter + Clone + 'static> EventsWriterClone for T {
    fn clone_boxed(&self) -> Box<dyn EventsWriter> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn EventsWriter> {
    fn clone(&self) -> Self {
        self.clone_boxed()
    }
}

/// EventsManager owns event handlers. Handlers are Trait objects, hence they have to be passed in a
/// Box. On handle_event all handler's handle_event methods are called.
impl EventsPublisher {
    pub fn new() -> Self {
        EventsPublisher {
            handlers: Vec::new(),
        }
    }

    pub fn add_subscriber(&mut self, handler: Box<dyn EventsSubscriber + Send>) {
        self.handlers.push(handler);
    }

    pub fn publish_event(&mut self, time: u32, event: &Event) {
        for handler in self.handlers.iter_mut() {
            handler.receive_event(time, event);
        }
    }

    #[instrument(skip_all, level = "trace")]
    pub fn finish(&mut self) {
        for handler in self.handlers.iter_mut() {
            handler.finish();
        }
    }

    pub fn get_subscriber<T: EventsSubscriber + 'static>(&mut self) -> Option<&mut T> {
        let mut result = None;
        for handler in self.handlers.iter_mut() {
            if let Some(collector) = handler.as_any().downcast_mut::<T>() {
                result = Some(collector)
            };
        }
        result
    }
}

impl Event {
    pub fn new_generic(event_type: &str, attrs: HashMap<String, String>) -> Event {
        Event {
            r#type: Some(Generic(GenericEvent {
                r#type: String::from(event_type),
                attrs,
            })),
        }
    }

    /// Prost only allows owned values in messags. Therefore we have to pass
    /// act_type as owned string. We only have a few act_types in our simulation
    /// but a lot of act events. This has to be done differently somehow.
    /// Quick-protobuf crate allows for Cow reference for example.
    pub fn new_act_start(person: u64, link: u64, act_type: u64) -> Event {
        Event {
            r#type: Some(ActStart(ActivityStartEvent {
                person,
                link,
                act_type,
            })),
        }
    }

    pub fn new_act_end(person: u64, link: u64, act_type: u64) -> Event {
        Event {
            r#type: Some(ActEnd(ActivityEndEvent {
                person,
                link,
                act_type,
            })),
        }
    }

    pub fn new_link_enter(link: u64, vehicle: u64) -> Event {
        Event {
            r#type: Some(LinkEnter(LinkEnterEvent { link, vehicle })),
        }
    }

    pub fn new_link_leave(link: u64, vehicle: u64) -> Event {
        Event {
            r#type: Some(LinkLeave(LinkLeaveEvent { link, vehicle })),
        }
    }

    pub fn new_person_enters_veh(person: u64, vehicle: u64) -> Event {
        Event {
            r#type: Some(PersonEntersVeh(PersonEntersVehicleEvent {
                person,
                vehicle,
            })),
        }
    }

    pub fn new_person_leaves_veh(person: u64, vehicle: u64) -> Event {
        Event {
            r#type: Some(PersonLeavesVeh(PersonLeavesVehicleEvent {
                person,
                vehicle,
            })),
        }
    }

    pub fn new_departure(person: u64, link: u64, leg_mode: u64) -> Event {
        Event {
            r#type: Some(Departure(DepartureEvent {
                person,
                link,
                leg_mode,
            })),
        }
    }

    pub fn new_arrival(person: u64, link: u64, leg_mode: u64) -> Event {
        Event {
            r#type: Some(Arrival(ArrivalEvent {
                person,
                link,
                leg_mode,
            })),
        }
    }

    pub fn new_travelled(person: u64, distance: f64, mode: u64) -> Event {
        Event {
            r#type: Some(Travelled(TravelledEvent {
                person,
                distance,
                mode,
            })),
        }
    }
}

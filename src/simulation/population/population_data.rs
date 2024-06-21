use std::collections::HashMap;
use tracing::debug;

use crate::simulation::id::Id;
use crate::simulation::io::attributes::Attrs;
use crate::simulation::network::global_network::Link;
use crate::simulation::population::io::{
    IOActivity, IOLeg, IOPerson, IOPlan, IOPlanElement, IORoute,
};
use crate::simulation::time_queue::EndTime;
use crate::simulation::vehicles::garage::Garage;
use crate::simulation::wire_types::messages::Vehicle;
use crate::simulation::wire_types::population::{Activity, Leg, Person, Plan, Route};
use crate::simulation::wire_types::vehicles::VehicleType;

pub enum State {
    ACTIVITY,
    LEG,
}

impl Person {
    pub fn from_io(io_person: &IOPerson) -> Person {
        let person_id = Id::get_from_ext(&io_person.id);

        let plan = Plan::from_io(io_person.selected_plan(), &person_id);

        if plan.acts.is_empty() {
            debug!("There is an empty plan for person {:?}", io_person.id);
        }

        Person {
            id: person_id.internal(),
            plan: Some(plan),
            curr_plan_elem: 0,
        }
    }

    pub fn new(id: u64, plan: Plan) -> Self {
        Person {
            id,
            curr_plan_elem: 0,
            plan: Some(plan),
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn state(&self) -> State {
        if self.curr_plan_elem % 2 == 0 {
            State::ACTIVITY
        } else {
            State::LEG
        }
    }

    pub fn add_act_after_curr(&mut self, to_add: Vec<Activity>) {
        let next_act_index = self.next_act_index() as usize;
        self.plan
            .as_mut()
            .unwrap()
            .acts
            .splice(next_act_index..next_act_index, to_add);
    }

    pub fn replace_next_leg(&mut self, to_add: Vec<Leg>) {
        let next_leg_index = self.next_leg_index() as usize;
        //remove next leg
        self.plan.as_mut().unwrap().legs.remove(next_leg_index);

        //insert legs at next leg index
        self.plan
            .as_mut()
            .unwrap()
            .legs
            .splice(next_leg_index..next_leg_index, to_add);
    }

    pub fn curr_act(&self) -> &Activity {
        if self.curr_plan_elem % 2 != 0 {
            panic!("Current element is not an activity");
        }
        let act_index = self.curr_plan_elem / 2;
        self.get_act_at_index(act_index)
    }

    pub fn previous_act(&self) -> &Activity {
        let act_index = self.next_act_index() - 1;
        self.get_act_at_index(act_index)
    }

    pub fn curr_act_mut(&mut self) -> &mut Activity {
        if self.curr_plan_elem % 2 != 0 {
            panic!("Current element is not an activity");
        }
        let act_index = self.curr_plan_elem / 2;
        self.get_act_at_index_mut(act_index)
    }

    pub fn next_act(&self) -> &Activity {
        let act_index = self.next_act_index();
        self.get_act_at_index(act_index)
    }

    pub fn next_act_mut(&mut self) -> &mut Activity {
        let act_index = self.next_act_index();
        self.get_act_at_index_mut(act_index)
    }

    fn next_act_index(&self) -> u32 {
        match self.curr_plan_elem % 2 {
            //current element is an activity => two elements after is the next activity
            0 => (self.curr_plan_elem + 2) / 2,
            //current element is a leg => one element after is the next activity
            1 => (self.curr_plan_elem + 1) / 2,
            _ => {
                panic!(
                    "There was an error while getting the next activity of Person {:?}",
                    self.id
                )
            }
        }
    }

    pub fn curr_leg(&self) -> &Leg {
        if self.curr_plan_elem % 2 != 1 {
            panic!("Current element is not a leg.");
        }

        let leg_index = (self.curr_plan_elem - 1) / 2;
        self.plan
            .as_ref()
            .unwrap()
            .legs
            .get(leg_index as usize)
            .unwrap()
    }

    pub fn previous_leg(&self) -> &Leg {
        let leg_index = self.next_leg_index() - 1;
        self.get_leg_at_index(leg_index)
    }

    pub fn next_leg(&self) -> &Leg {
        let next_leg_index = self.next_leg_index();
        self.get_leg_at_index(next_leg_index)
    }

    fn next_leg_mut(&mut self) -> &mut Leg {
        let next_leg_index = self.next_leg_index();
        self.get_leg_at_index_mut(next_leg_index)
    }

    fn next_leg_index(&self) -> u32 {
        match self.curr_plan_elem % 2 {
            //current element is an activity => one element after is the next leg
            0 => (self.curr_plan_elem + 1) / 2,
            //current element is a leg => two elements after is the next leg
            1 => (self.curr_plan_elem + 2) / 2,
            _ => {
                panic!(
                    "There was an error while getting the next leg of Person {:?}",
                    self.id
                )
            }
        }
    }

    fn get_act_at_index(&self, index: u32) -> &Activity {
        self.plan
            .as_ref()
            .unwrap()
            .acts
            .get(index as usize)
            .unwrap()
    }

    fn get_act_at_index_mut(&mut self, index: u32) -> &mut Activity {
        self.plan
            .as_mut()
            .unwrap()
            .acts
            .get_mut(index as usize)
            .unwrap()
    }

    fn get_leg_at_index(&self, index: u32) -> &Leg {
        self.plan
            .as_ref()
            .unwrap()
            .legs
            .get(index as usize)
            .unwrap()
    }

    fn get_leg_at_index_mut(&mut self, index: u32) -> &mut Leg {
        self.plan
            .as_mut()
            .unwrap()
            .legs
            .get_mut(index as usize)
            .unwrap()
    }

    pub fn update_next_leg(
        &mut self,
        dep_time: Option<u32>,
        travel_time: u32,
        route: Vec<u64>,
        distance: f64,
        vehicle_id: u64,
    ) {
        let next_leg = self.next_leg_mut();

        let simulation_route = Route {
            veh_id: vehicle_id,
            distance,
            route,
        };

        next_leg.dep_time = dep_time;
        next_leg.trav_time = travel_time;
        next_leg.route = Some(simulation_route);
    }

    pub fn advance_plan(&mut self) {
        let next = self.curr_plan_elem + 1;
        if self.plan.as_ref().unwrap().acts.len() + self.plan.as_ref().unwrap().legs.len()
            == next as usize
        {
            panic!(
                "Person: Advance plan was called on Person #{}, but no element is remaining.",
                self.id
            )
        }
        self.curr_plan_elem = next;
    }
}

impl EndTime for Person {
    fn end_time(&self, now: u32) -> u32 {
        return if self.curr_plan_elem % 2 == 0 {
            self.curr_act().cmp_end_time(now)
        } else {
            self.curr_leg().trav_time + now
        };
    }
}

impl Plan {
    pub const DEFAULT_ROUTING_MODE: &'static str = "car";

    pub fn new() -> Plan {
        Plan {
            acts: Vec::new(),
            legs: Vec::new(),
        }
    }

    fn from_io(io_plan: &IOPlan, person_id: &Id<Person>) -> Plan {
        assert!(!io_plan.elements.is_empty());
        if let IOPlanElement::Leg(_leg) = io_plan.elements.get(0).unwrap() {
            panic!("First plan element must be an activity! But was a leg.");
        };

        let mut result = Plan::new();

        for element in &io_plan.elements {
            match element {
                IOPlanElement::Activity(io_act) => {
                    let act = Activity::from_io(io_act);
                    result.acts.push(act);
                }
                IOPlanElement::Leg(io_leg) => {
                    let leg = Leg::from_io(io_leg, person_id);
                    result.legs.push(leg);
                }
            }
        }

        if result.acts.len() - result.legs.len() != 1 {
            panic!("Plan {:?} has less legs than expected", io_plan);
        }

        result
    }

    pub fn add_leg(&mut self, leg: Leg) {
        self.legs.push(leg);
    }

    pub fn add_act(&mut self, activity: Activity) {
        self.acts.push(activity);
    }
}

impl Activity {
    fn from_io(io_act: &IOActivity) -> Self {
        let link_id: Id<Link> = Id::get_from_ext(&io_act.link);
        let act_type: Id<String> = Id::get_from_ext(&io_act.r#type);
        Activity {
            x: io_act.x,
            y: io_act.y,
            act_type: act_type.internal(),
            link_id: link_id.internal(),
            start_time: parse_time_opt(&io_act.start_time),
            end_time: parse_time_opt(&io_act.end_time),
            max_dur: parse_time_opt(&io_act.max_dur),
        }
    }

    pub fn new(
        x: f64,
        y: f64,
        act_type: u64,
        link_id: u64,
        start_time: Option<u32>,
        end_time: Option<u32>,
        max_dur: Option<u32>,
    ) -> Self {
        Activity {
            x,
            y,
            act_type,
            link_id,
            start_time,
            end_time,
            max_dur,
        }
    }

    pub fn interaction(link_id: u64, act_type: u64) -> Activity {
        Activity {
            act_type,
            link_id,
            x: 0.0, //dummy value which is never evaluated again
            y: 0.0, //dummy value which is never evaluated again
            start_time: None,
            end_time: None,
            max_dur: Some(0),
        }
    }

    fn cmp_end_time(&self, now: u32) -> u32 {
        if let Some(end_time) = self.end_time {
            end_time
        } else if let Some(max_dur) = self.max_dur {
            now + max_dur
        } else {
            // supposed to be an equivalent for OptionalTime.undefined() in the java code
            u32::MAX
        }
    }

    pub fn is_interaction(&self) -> bool {
        Id::<String>::get(self.act_type)
            .external()
            .contains("interaction")
    }
}

impl Leg {
    pub const PASSENGER_ID_ATTRIBUTE: &'static str = "passenger_id";

    fn from_io(io_leg: &IOLeg, person_id: &Id<Person>) -> Self {
        let routing_mode_ext = Attrs::find_or_else_opt(&io_leg.attributes, "routingMode", || "car");

        let routing_mode: Id<String> = Id::create(routing_mode_ext);
        let mode = Id::get_from_ext(io_leg.mode.as_str());
        let route = Route::from_io(&io_leg.route, person_id, &mode);

        Self {
            route: Some(route),
            mode: mode.internal(),
            trav_time: Self::parse_trav_time(&io_leg.trav_time, &io_leg.route.trav_time),
            dep_time: parse_time_opt(&io_leg.dep_time),
            routing_mode: routing_mode.internal(),
            attributes: HashMap::new(),
        }
    }

    pub fn new(route: Route, mode: u64, trav_time: u32, dep_time: Option<u32>) -> Self {
        Self {
            route: Some(route),
            mode,
            trav_time,
            dep_time,
            routing_mode: 0,
            attributes: HashMap::new(),
        }
    }

    pub fn access_eggress(net_mode: u64, veh_type_id: u64) -> Self {
        Leg {
            mode: net_mode,
            routing_mode: net_mode,
            dep_time: None,
            trav_time: 0,
            route: Some(Route {
                veh_id: veh_type_id,
                distance: 0.0,
                route: Vec::new(),
            }),
            attributes: HashMap::new(),
        }
    }

    fn parse_trav_time(leg_trav_time: &Option<String>, route_trav_time: &Option<String>) -> u32 {
        if let Some(trav_time) = parse_time_opt(leg_trav_time) {
            trav_time
        } else if let Some(trav_time) = parse_time_opt(route_trav_time) {
            trav_time
        } else {
            0
        }
    }

    pub fn vehicle_type_id<'gar>(&self, garage: &'gar Garage) -> &'gar Id<VehicleType> {
        self.route
            .as_ref()
            .map(|r| garage.vehicles.get(&Id::<Vehicle>::get(r.veh_id)).unwrap())
            .unwrap()
    }
}

impl Route {
    pub fn start_link(&self) -> u64 {
        *self.route.first().unwrap()
    }

    pub fn end_link(&self) -> u64 {
        *self.route.last().unwrap()
    }

    fn from_io(io_route: &IORoute, person_id: &Id<Person>, mode: &Id<String>) -> Self {
        let route = match io_route.r#type.as_str() {
            "generic" => Self::from_io_generic(io_route, person_id, mode),
            "links" => Self::from_io_net_route(io_route, person_id, mode),
            _t => panic!("Unsupported route type: '{_t}'"),
        };

        route
    }

    fn from_io_generic(io_route: &IORoute, person_id: &Id<Person>, mode: &Id<String>) -> Self {
        let start_link: Id<Link> = Id::get_from_ext(&io_route.start_link);
        let end_link: Id<Link> = Id::get_from_ext(&io_route.end_link);
        let external = format!("{}_{}", person_id.external(), mode.external());
        let veh_id: Id<Vehicle> = Id::get_from_ext(&external);

        Route {
            distance: io_route.distance,
            veh_id: veh_id.internal(),
            route: vec![start_link.internal(), end_link.internal()],
        }
    }

    fn from_io_net_route(io_route: &IORoute, person_id: &Id<Person>, mode: &Id<String>) -> Self {
        if let Some(veh_id_ext) = &io_route.vehicle {
            // catch this special case because we have "null" as vehicle ids for modes which are
            // routed but not simulated on the network.
            if veh_id_ext.eq("null") {
                Self::from_io_generic(io_route, person_id, mode)
            } else {
                let veh_id: Id<Vehicle> = Id::get_from_ext(veh_id_ext.as_str());
                let link_ids = match &io_route.route {
                    None => Vec::new(),
                    Some(encoded_links) => encoded_links
                        .split(' ')
                        .map(|matsim_id| Id::<Link>::get_from_ext(matsim_id).internal())
                        .collect(),
                };
                Route {
                    distance: io_route.distance,
                    veh_id: veh_id.internal(),
                    route: link_ids,
                }
            }
        } else {
            panic!("vehicle id is expected to be set. ")
        }
    }
}

fn parse_time_opt(value: &Option<String>) -> Option<u32> {
    if let Some(time) = value.as_ref() {
        parse_time(time)
    } else {
        None
    }
}

fn parse_time(value: &str) -> Option<u32> {
    let split: Vec<&str> = value.split(':').collect();
    if split.len() == 3 {
        let hour: u32 = split.first().unwrap().parse().unwrap();
        let minutes: u32 = split.get(1).unwrap().parse().unwrap();
        let seconds: u32 = split.get(2).unwrap().parse().unwrap();

        Some(hour * 3600 + minutes * 60 + seconds)
    } else {
        None
    }
}

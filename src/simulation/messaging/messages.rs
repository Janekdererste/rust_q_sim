use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::Cursor;

use prost::Message;
use tracing::debug;

use crate::simulation::config::RoutingMode;
use crate::simulation::id_mapping::{MatsimIdMapping, MatsimIdMappings};
use crate::simulation::io::matsim_id::MatsimId;
use crate::simulation::io::population::{
    IOActivity, IOLeg, IOPerson, IOPlan, IOPlanElement, IORoute,
};
use crate::simulation::messaging::messages::proto::leg::Route;
use crate::simulation::messaging::messages::proto::{
    Activity, Agent, ExperimentalMessage, GenericRoute, Leg, NetworkRoute, Plan,
    TravelTimesMessage, Vehicle, VehicleMessage, VehicleType,
};
use crate::simulation::time_queue::EndTime;

// Include the `messages` module, which is generated from messages.proto.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/mpi.messages.rs"));
}

impl ExperimentalMessage {
    pub fn new() -> ExperimentalMessage {
        ExperimentalMessage {
            counter: 0,
            timestamp: 0,
            additional_message: String::new(),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode(&mut buf).unwrap();
        buf
    }

    pub fn deserialize(buf: &[u8]) -> ExperimentalMessage {
        ExperimentalMessage::decode(&mut Cursor::new(buf)).unwrap()
    }
}

impl VehicleMessage {
    pub fn new(time: u32, from: u32, to: u32) -> VehicleMessage {
        VehicleMessage {
            time,
            from_process: from as u32,
            to_process: to as u32,
            vehicles: Vec::new(),
        }
    }

    pub fn add(&mut self, vehicle: Vehicle) {
        self.vehicles.push(vehicle);
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(self.encoded_len());
        self.encode(&mut buffer).unwrap();
        buffer
    }

    pub fn deserialize(buffer: &[u8]) -> VehicleMessage {
        VehicleMessage::decode(&mut Cursor::new(buffer)).unwrap()
    }
}

impl TravelTimesMessage {
    pub fn new() -> Self {
        TravelTimesMessage {
            travel_times_by_link_id: HashMap::new(),
        }
    }

    pub fn from(map: HashMap<u64, u32>) -> Self {
        TravelTimesMessage {
            travel_times_by_link_id: map,
        }
    }

    pub fn add_travel_time(&mut self, link: u64, travel_time: u32) {
        self.travel_times_by_link_id.insert(link, travel_time);
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(self.encoded_len());
        self.encode(&mut buffer).unwrap();
        buffer
    }

    pub fn deserialize(buffer: &[u8]) -> TravelTimesMessage {
        TravelTimesMessage::decode(&mut Cursor::new(buffer)).unwrap()
    }
}

// Implementation for ordering, so that vehicle messages can be put into a message queue sorted by time
impl PartialOrd for VehicleMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for VehicleMessage {}

impl Ord for VehicleMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        other.time.cmp(&self.time)
    }
}

impl Vehicle {
    pub fn new(id: u64, veh_type: VehicleType, mode: String, agent: Agent) -> Vehicle {
        Vehicle {
            id,
            agent: Some(agent),
            curr_route_elem: 0,
            r#type: veh_type as i32,
            mode,
        }
    }

    fn agent(&self) -> &Agent {
        self.agent.as_ref().unwrap()
    }

    pub fn id(&self) -> usize {
        self.id as usize
    }

    pub fn advance_route_index(&mut self) {
        self.curr_route_elem += 1;
    }

    pub fn curr_link_id(&self) -> Option<usize> {
        let leg = self.agent().curr_leg();
        let route = leg.route.as_ref().unwrap();
        match route {
            Route::GenericRoute(route) => Some(route.end_link as usize),
            Route::NetworkRoute(route) => {
                let index = self.curr_route_elem as usize;
                route.route.get(index).map(|id| *id as usize)
            }
        }
    }

    pub fn is_current_link_last(&self) -> bool {
        let leg = self.agent().curr_leg();
        let route = leg.route.as_ref().unwrap();
        match route {
            Route::GenericRoute(_) => true,
            Route::NetworkRoute(route) => self.curr_route_elem + 1 >= route.route.len() as u32,
        }
    }

    pub fn mode(&self) -> &str {
        self.mode.as_str()
    }
}

impl EndTime for Vehicle {
    fn end_time(&self, now: u32) -> u32 {
        self.agent().end_time(now)
    }
}

impl Agent {
    pub fn from_io(
        io_person: &IOPerson,
        id_mappings: &MatsimIdMappings,
        routing_mode: RoutingMode,
    ) -> Agent {
        let plan = Plan::from_io(io_person.selected_plan(), id_mappings, routing_mode);

        if plan.acts.is_empty() {
            debug!("There is an empty plan for person {:?}", io_person.id);
        }

        if plan.acts.len() == 1 {
            debug!(
                "There is a plan with one activity only for person {:?}",
                io_person.id
            );
        }

        let id = *id_mappings.agents.get_internal(io_person.id()).unwrap();
        Agent {
            id: id as u64,
            plan: Some(plan),
            curr_plan_elem: 0,
        }
    }

    pub fn new(id: u64, plan: Plan) -> Self {
        Agent {
            id,
            curr_plan_elem: 0,
            plan: Some(plan),
        }
    }

    pub fn id(&self) -> usize {
        self.id as usize
    }

    pub fn curr_act(&self) -> &Activity {
        if self.curr_plan_elem % 2 != 0 {
            panic!("Current element is not an activity");
        }
        let act_index = self.curr_plan_elem / 2;
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
        let act_index = match self.curr_plan_elem % 2 {
            //current element is an activity => two elements after is the next activity
            0 => (self.curr_plan_elem + 2) / 2,
            //current element is a leg => one element after is the next activity
            1 => (self.curr_plan_elem + 1) / 2,
            _ => {
                panic!(
                    "There was an error while getting the next activity of agent {:?}",
                    self.id
                )
            }
        };
        act_index
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

    pub fn next_leg(&self) -> &Leg {
        let next_leg_index = self.next_leg_index();
        self.get_leg_at_index(next_leg_index)
    }

    fn next_leg_mut(&mut self) -> &mut Leg {
        let next_leg_index = self.next_leg_index();
        self.get_leg_at_index_mut(next_leg_index)
    }

    fn next_leg_index(&self) -> u32 {
        let next_leg_index = match self.curr_plan_elem % 2 {
            //current element is an activity => one element after is the next leg
            0 => (self.curr_plan_elem + 1) / 2,
            //current element is a leg => two elements after is the next leg
            1 => (self.curr_plan_elem + 2) / 2,
            _ => {
                panic!(
                    "There was an error while getting the next leg of agent {:?}",
                    self.id
                )
            }
        };
        next_leg_index
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
        travel_time: Option<u32>,
        route: Vec<u64>,
        distance: Option<f32>,
        start_link: u64,
        end_link: u64,
    ) {
        //info!("Leg update for agent {:?}. Departure {:?}, travel time {:?}, route {:?}, distance {:?}, start_link {:?}, end_link {:?}",
        //    self, dep_time, travel_time, route,distance, start_link, end_link);

        let simulation_route = match route.is_empty() {
            true => Route::GenericRoute(GenericRoute {
                start_link,
                end_link,
                trav_time: travel_time.expect("No travel time set for walking leg."),
                distance: distance.expect("No distance set for walking leg."),
            }),
            false => Route::NetworkRoute(NetworkRoute {
                vehicle_id: self.id,
                route,
            }),
        };

        let next_leg = self.next_leg_mut();

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
                "Agent: Advance plan was called on agent #{}, but no element is remaining.",
                self.id
            )
        }
        self.curr_plan_elem = next;
    }
}

impl EndTime for Agent {
    fn end_time(&self, now: u32) -> u32 {
        return if self.curr_plan_elem % 2 == 0 {
            self.curr_act().cmp_end_time(now)
        } else {
            let route = self.curr_leg().route.as_ref().unwrap();
            match route {
                Route::GenericRoute(gen_route) => now + gen_route.trav_time,
                Route::NetworkRoute(_) => {
                    panic!("End time not supported for network route")
                }
            }
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

    fn from_io(
        io_plan: &IOPlan,
        id_mappings: &MatsimIdMappings,
        routing_mode: RoutingMode,
    ) -> Plan {
        assert!(!io_plan.elements.is_empty());
        if let IOPlanElement::Leg(_leg) = io_plan.elements.get(0).unwrap() {
            panic!("First plan element must be an activity! But was a leg.");
        };

        match routing_mode {
            RoutingMode::UsePlans => Plan::get_full_plan_no_routing(io_plan, id_mappings),
            RoutingMode::AdHoc => Plan::get_full_plan_for_routing(io_plan, id_mappings),
        }
    }

    fn get_full_plan_no_routing(io_plan: &IOPlan, id_mappings: &MatsimIdMappings) -> Plan {
        let mut result = Plan::new();

        for element in &io_plan.elements {
            match element {
                IOPlanElement::Activity(io_act) => {
                    let act = Activity::from_io(io_act, &id_mappings.links);
                    result.acts.push(act);
                }
                IOPlanElement::Leg(io_leg) => {
                    let leg = Leg::from_io(io_leg, id_mappings);
                    result.legs.push(leg);
                }
            }
        }

        if result.acts.len() - result.legs.len() != 1 {
            panic!("Plan {:?} has less legs than expected", io_plan);
        }

        result
    }

    fn get_full_plan_for_routing(io_plan: &IOPlan, id_mappings: &MatsimIdMappings) -> Plan {
        if io_plan.elements.is_empty() {
            return Plan::new();
        }

        if io_plan.elements.len() == 1 {
            let mut plan = Plan::new();
            if let IOPlanElement::Activity(io_activity) = io_plan.elements.get(0).unwrap() {
                plan.acts
                    .push(Activity::from_io(io_activity, &id_mappings.links));
            } else {
                panic!("The first element of a plan has to be an activity.")
            }
            return plan;
        }

        let plan_type = Plan::get_plan_type(io_plan);
        let window_size = plan_type.window_size();
        let step_size = plan_type.step_size();
        assert_eq!(
            (io_plan.elements.len() - 1) % step_size,
            0,
            "The number of elements in the plan is wrong."
        );

        let mut result = Plan::new();

        let plan_windows = io_plan.elements.windows(window_size);
        let number_of_plan_windows = plan_windows.len();
        for (i, window) in plan_windows.into_iter().step_by(step_size).enumerate() {
            let curr_activity = IOPlanElement::get_activity(window.first());
            let next_activity = IOPlanElement::get_activity(window.last());
            let mut access_walk = None;
            let mut access_interaction = None;
            let mut main_leg = None;
            let mut egress_interaction = None;
            let mut egress_walk = None;

            if window_size == 3 {
                main_leg = IOPlanElement::get_leg(window.get(1));
            }

            if window_size == 7 {
                access_walk = IOPlanElement::get_leg(window.get(1));
                access_interaction = IOPlanElement::get_activity(window.get(2));
                main_leg = IOPlanElement::get_leg(window.get(3));
                egress_interaction = IOPlanElement::get_activity(window.get(4));
                egress_walk = IOPlanElement::get_leg(window.get(5));
            }

            let curr_act_link_id = *id_mappings
                .links
                .get_internal(curr_activity.unwrap().link.as_str())
                .unwrap() as u64;
            let next_act_link_id = *id_mappings
                .links
                .get_internal(next_activity.unwrap().link.as_str())
                .unwrap() as u64;

            // current activity
            let act = Activity::from_io(curr_activity.unwrap(), &id_mappings.links);
            result.acts.push(act);

            // access walk and interaction
            Self::insert_access_or_egress(
                id_mappings,
                &mut result,
                access_walk,
                access_interaction,
                curr_act_link_id,
            );

            // main leg
            Self::insert_main_leg(&mut result, main_leg);

            // egress interaction and walk
            Self::insert_access_or_egress(
                id_mappings,
                &mut result,
                egress_walk,
                egress_interaction,
                next_act_link_id,
            );

            // last activity
            if i == number_of_plan_windows - 1 {
                let act = Activity::from_io(next_activity.unwrap(), &id_mappings.links);
                result.acts.push(act);
            }
        }
        result
    }

    pub fn add_leg(&mut self, leg: Leg) {
        self.legs.push(leg);
    }

    pub fn add_act(&mut self, activity: Activity) {
        self.acts.push(activity);
    }

    fn get_plan_type(io_plan: &IOPlan) -> PlanType {
        if let IOPlanElement::Activity(_) = io_plan.elements.get(1).unwrap() {
            return PlanType::ActivitiesOnly;
        }

        if let IOPlanElement::Activity(a) = io_plan.elements.get(2).unwrap() {
            return if a.is_interaction() {
                PlanType::ActivitiesAndMainLegsWithInteractionAndWalk
            } else {
                PlanType::ActivitiesAndMainLeg
            };
        } else {
            panic!("The third element should never be a leg.")
        }
    }

    fn insert_main_leg(result: &mut Plan, main_leg: Option<&IOLeg>) {
        if main_leg.is_some() {
            result
                .legs
                .push(Leg::only_with_mode(main_leg.unwrap().mode.as_str()))
        } else {
            result
                .legs
                .push(Leg::only_with_mode(Plan::DEFAULT_ROUTING_MODE))
        }
    }

    fn insert_access_or_egress(
        id_mappings: &MatsimIdMappings,
        result: &mut Plan,
        leg: Option<&IOLeg>,
        interaction: Option<&IOActivity>,
        activity_link_id: u64,
    ) {
        if leg.is_some() && interaction.is_some() {
            result.legs.push(Leg::from_io(leg.unwrap(), id_mappings));
            result
                .acts
                .push(Activity::from_io(interaction.unwrap(), &id_mappings.links))
        } else {
            let access_walk_leg = Leg::only_with_mode("walk");
            result.legs.push(access_walk_leg);

            let access_interaction_act = Activity {
                act_type: String::from(Plan::DEFAULT_ROUTING_MODE) + " interaction",
                link_id: activity_link_id,
                x: 0.0, //dummy value which is never evaluated again
                y: 0.0, //dummy value which is never evaluated again
                start_time: None,
                end_time: None,
                max_dur: Some(0),
            };
            result.acts.push(access_interaction_act);
        }
    }
}

enum PlanType {
    ActivitiesOnly,
    ActivitiesAndMainLeg,
    ActivitiesAndMainLegsWithInteractionAndWalk,
}

impl PlanType {
    fn window_size(&self) -> usize {
        match self {
            PlanType::ActivitiesOnly => 2,
            PlanType::ActivitiesAndMainLeg => 3,
            PlanType::ActivitiesAndMainLegsWithInteractionAndWalk => 7,
        }
    }

    fn step_size(&self) -> usize {
        self.window_size() - 1
    }
}

impl Activity {
    fn from_io(io_act: &IOActivity, link_id_mapping: &MatsimIdMapping) -> Self {
        let link_id = *link_id_mapping.get_internal(io_act.link.as_str()).unwrap();
        Activity {
            x: io_act.x,
            y: io_act.y,
            act_type: io_act.r#type.clone(),
            link_id: link_id as u64,
            start_time: parse_time_opt(&io_act.start_time),
            end_time: parse_time_opt(&io_act.end_time),
            max_dur: parse_time_opt(&io_act.max_dur),
        }
    }

    pub fn new(
        x: f32,
        y: f32,
        act_type: String,
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
        self.act_type.contains("interaction")
    }
}

impl Leg {
    fn from_io(io_leg: &IOLeg, id_mappings: &MatsimIdMappings) -> Self {
        let route = Route::from_io(&io_leg.route, id_mappings);
        Self {
            route: Some(route),
            mode: io_leg.mode.clone(),
            trav_time: parse_time_opt(&io_leg.trav_time),
            dep_time: parse_time_opt(&io_leg.dep_time),
        }
    }

    pub fn new(route: Route, mode: &str, trav_time: Option<u32>, dep_time: Option<u32>) -> Self {
        Self {
            route: Some(route),
            mode: String::from(mode),
            trav_time,
            dep_time,
        }
    }

    fn only_with_mode(mode: &str) -> Self {
        Self {
            mode: mode.to_string(),
            dep_time: None,
            trav_time: None,
            route: None,
        }
    }
}

impl Route {
    fn from_io(io_route: &IORoute, id_mappings: &MatsimIdMappings) -> Self {
        match io_route.r#type.as_str() {
            "generic" => Route::GenericRoute(GenericRoute::from_io(io_route, &id_mappings.links)),
            "links" => {
                if let Some(vehicle_id) = &io_route.vehicle {
                    // catch this special case because we have "null" as vehicle ids for modes which are
                    // routed but not simulated on the network.
                    if vehicle_id.eq("null") {
                        Route::GenericRoute(GenericRoute::from_io(io_route, &id_mappings.links))
                    } else {
                        Route::NetworkRoute(NetworkRoute::from_io(io_route, id_mappings))
                    }
                } else {
                    panic!("vehicle id is expected to be set. ")
                }
            }
            _t => panic!("Unsupported route type: '{_t}'"),
        }
    }
}

impl GenericRoute {
    fn from_io(io_route: &IORoute, link_id_mapping: &MatsimIdMapping) -> Self {
        let start_link = *link_id_mapping
            .get_internal(io_route.start_link.as_str())
            .unwrap();
        let end_link = *link_id_mapping
            .get_internal(io_route.end_link.as_str())
            .unwrap();
        let trav_time = parse_time_opt(&io_route.trav_time).unwrap();

        Self {
            start_link: start_link as u64,
            end_link: end_link as u64,
            trav_time,
            distance: io_route.distance,
        }
    }
}

impl NetworkRoute {
    fn from_io(io_route: &IORoute, id_mappings: &MatsimIdMappings) -> Self {
        let matsim_veh_id = io_route
            .vehicle
            .as_ref()
            .unwrap_or_else(|| panic!("Couldn't find veh-id for route {io_route:?} "));
        let veh_id = id_mappings
            .vehicles
            .get_internal(matsim_veh_id.as_str())
            .unwrap_or_else(|| {
                panic!("Couldn't find veh-id: {matsim_veh_id:?} for route {io_route:?}")
            });
        let link_ids = match &io_route.route {
            None => Vec::new(),
            Some(encoded_links) => encoded_links
                .split(' ')
                .map(|matsim_id| *id_mappings.links.get_internal(matsim_id).unwrap() as u64)
                .collect(),
        };
        Self {
            route: link_ids,
            vehicle_id: *veh_id as u64,
        }
    }

    pub fn new(vehicle_id: u64, link_ids: Vec<u64>) -> Self {
        Self {
            vehicle_id,
            route: link_ids,
        }
    }
}

fn parse_time_opt(value: &Option<String>) -> Option<u32> {
    value.as_ref().map(|value| parse_time(value))
}

fn parse_time(value: &str) -> u32 {
    let split: Vec<&str> = value.split(':').collect();
    assert_eq!(3, split.len());

    let hour: u32 = split.first().unwrap().parse().unwrap();
    let minutes: u32 = split.get(1).unwrap().parse().unwrap();
    let seconds: u32 = split.get(2).unwrap().parse().unwrap();

    hour * 3600 + minutes * 60 + seconds
}

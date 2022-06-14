use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::container::population::{Activity, Leg, Person, Plan, PlanElement, Population, Route};
use crate::simulation::q_network::QNetwork;

#[derive(Debug)]
struct QPopulation {
    persons: BinaryHeap<Agent>,
}

impl QPopulation {
    fn new() -> QPopulation {
        QPopulation {
            persons: BinaryHeap::new(),
        }
    }

    pub fn from_container(population: &Population, q_network: &QNetwork) -> QPopulation {
        let mut result = QPopulation::new();

        // go over all the persons
        for person in &population.persons {
            let next_id = result.persons.len();
            let agent = Agent::from_container(person, next_id, q_network);
            result.persons.push(agent);
        }
        result
    }
}

#[derive(Debug)]
struct Agent {
    id: usize,
    plan: SimPlan,
    current_plan_element: usize,
    next_wakeup_time: i32,
}

impl Agent {
    fn from_container(person: &Person, id: usize, q_network: &QNetwork) -> Agent {
        let plan = SimPlan::from_container(person.selected_plan(), q_network);
        let current_plan_element: usize = 0;

        // now, figure out the first wakeup time
        let first_element = plan.elements.get(0).unwrap();

        let wakeup_time = match first_element {
            SimPlanElement::Activity(act) => act.end_time(0),
            _ => panic!("First element was a Leg. This is not allowed."),
        };

        Agent {
            id: id,
            plan,
            current_plan_element: 0,
            next_wakeup_time: wakeup_time,
        }
    }
}

impl PartialOrd for Agent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.next_wakeup_time.cmp(&other.next_wakeup_time))
    }
}

impl Ord for Agent {
    fn cmp(&self, other: &Self) -> Ordering {
        self.next_wakeup_time.cmp(&other.next_wakeup_time)
    }
}

impl PartialEq for Agent {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Agent {}

#[derive(Debug)]
struct SimPlan {
    elements: Vec<SimPlanElement>,
}

impl SimPlan {
    fn from_container(plan: &Plan, q_network: &QNetwork) -> SimPlan {
        // each plan needs at least one element
        assert!(plan.elements.len() > 0);
        if let PlanElement::Leg(_leg) = plan.elements.get(0).unwrap() {
            panic!("First plan element must be an activity! But was a leg.");
        }

        // convert plan elements into sim plan elements
        let sim_elements = plan
            .elements
            .iter()
            .map(|el| SimPlan::map_plan_element(el, q_network))
            .collect();

        SimPlan {
            elements: sim_elements,
        }
    }

    fn map_plan_element(element: &PlanElement, q_network: &QNetwork) -> SimPlanElement {
        match element {
            PlanElement::Activity(activity) => {
                SimPlanElement::Activity(SimActivity::from_container(activity, q_network))
            }
            PlanElement::Leg(leg) => SimPlanElement::Leg(SimLeg::from_container(leg, q_network)),
        }
    }
}

#[derive(Debug)]
enum SimPlanElement {
    Activity(SimActivity),
    Leg(SimLeg),
}

#[derive(Debug)]
struct SimActivity {
    act_type: String,
    link_id: usize,
    x: f32,
    y: f32,
    start_time: Option<i32>,
    end_time: Option<i32>,
    max_dur: Option<i32>,
}

impl SimActivity {
    fn from_container(activity: &Activity, q_network: &QNetwork) -> SimActivity {
        let link_id = q_network
            .link_id_mapping
            .get(activity.link.as_str())
            .unwrap();
        SimActivity {
            x: activity.x,
            y: activity.y,
            act_type: activity.r#type.clone(),
            link_id: *link_id,
            start_time: parse_time_opt(&activity.start_time),
            end_time: parse_time_opt(&activity.end_time),
            max_dur: parse_time_opt(&activity.max_dur),
        }
    }

    /**
    Calculates the end time of this activity. This only implements
    org.matsim.core.config.groups.PlansConfigGroup.ActivityDurationInterpretation.tryEndTimeThenDuration
     */
    fn end_time(&self, now: i32) -> i32 {
        if let Some(end_time) = self.end_time {
            end_time
        } else if let Some(max_dur) = self.max_dur {
            now + max_dur
        } else {
            // supposed to be an equivalent for OptionalTime.undefined() in the java code
            i32::MAX
        }
    }
}

#[derive(Debug)]
struct SimLeg {
    mode: String,
    dep_time: Option<i32>,
    trav_time: Option<i32>,
    route: SimRoute,
}

impl SimLeg {
    fn from_container(leg: &Leg, q_network: &QNetwork) -> SimLeg {
        let sim_route = SimLeg::map_route(&leg.route, q_network);

        SimLeg {
            mode: leg.mode.clone(),
            trav_time: parse_time_opt(&leg.trav_time),
            dep_time: parse_time_opt(&leg.dep_time),
            route: sim_route,
        }
    }

    fn map_route(route: &Route, q_network: &QNetwork) -> SimRoute {
        match route.r#type.as_str() {
            "generic" => SimRoute::GenericRoute(GenericRoute::from_container(route, q_network)),
            "links" => SimRoute::NetworkRoute(NetworkRoute::from_container(route, q_network)),
            _ => panic!("Unsupported route type: '{}'", route.r#type),
        }
    }
}

#[derive(Debug)]
enum SimRoute {
    NetworkRoute(NetworkRoute),
    GenericRoute(GenericRoute),
}

#[derive(Debug)]
struct GenericRoute {
    start_link: usize,
    end_link: usize,
    trav_time: i32,
    distance: f32,
}

impl GenericRoute {
    fn from_container(route: &Route, q_network: &QNetwork) -> GenericRoute {
        let start_link_id = q_network
            .link_id_mapping
            .get(route.start_link.as_str())
            .unwrap();
        let end_link_id = q_network
            .link_id_mapping
            .get(route.end_link.as_str())
            .unwrap();
        let trav_time = parse_time_opt(&route.trav_time).unwrap();

        GenericRoute {
            start_link: *start_link_id,
            end_link: *end_link_id,
            trav_time: trav_time,
            distance: route.distance,
        }
    }
}

#[derive(Debug)]
struct NetworkRoute {
    vehicle_id: String,
    route: Vec<usize>,
}

impl NetworkRoute {
    fn from_container(route: &Route, q_network: &QNetwork) -> NetworkRoute {
        let link_ids: Vec<usize> = route
            .route
            .as_ref()
            .unwrap()
            .split(' ')
            .map(|id| *q_network.link_id_mapping.get(id).unwrap())
            .collect();

        let vehicle_id = route.vehicle.as_ref().unwrap();

        NetworkRoute {
            vehicle_id: vehicle_id.clone(),
            route: link_ids,
        }
    }
}

fn parse_time_opt(value: &Option<String>) -> Option<i32> {
    match value {
        None => None,
        Some(value) => Some(parse_time(value)),
    }
}

fn parse_time(value: &str) -> i32 {
    let split: Vec<&str> = value.split(':').collect();
    assert_eq!(3, split.len());

    let hour: i32 = split.get(0).unwrap().parse().unwrap();
    let minutes: i32 = split.get(1).unwrap().parse().unwrap();
    let seconds: i32 = split.get(2).unwrap().parse().unwrap();

    hour * 3600 + minutes * 60 + seconds
}

#[cfg(test)]
mod tests {
    use crate::container::network::Network;
    use crate::container::population::Population;
    use crate::simulation::q_network::QNetwork;
    use crate::simulation::q_population::QPopulation;

    #[test]
    fn population_from_container() {
        let population: Population = Population::from_file("./assets/equil_output_plans.xml.gz");
        let network: Network = Network::from_file("./assets/equil-network.xml");
        let q_network: QNetwork = QNetwork::from_container(&network);
        let q_population = QPopulation::from_container(&population, &q_network);

        println!("{q_population:#?}");
    }
}

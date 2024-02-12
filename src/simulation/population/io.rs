use std::path::Path;

use serde::Deserialize;
use tracing::info;

use crate::simulation::id::Id;
use crate::simulation::io::attributes::Attrs;
use crate::simulation::io::proto::read_from_file;
use crate::simulation::io::xml;
use crate::simulation::population::population::Population;
use crate::simulation::vehicles::garage::Garage;
use crate::simulation::wire_types::population::Person;

pub fn from_file(path: &Path, garage: &mut Garage) -> Population {
    if path.extension().unwrap().eq("binpb") {
        load_from_proto(path)
    } else if path.extension().unwrap().eq("xml") || path.extension().unwrap().eq("gz") {
        load_from_xml(path, garage)
    } else {
        panic!("Tried to load {path:?}. File format not supported. Either use `.xml`, `.xml.gz`, or `.binpb` as extension");
    }
}

pub fn to_file(population: &Population, path: &Path) {
    if path.extension().unwrap().eq("binpb") {
        write_to_proto(population, path);
    } else if path.extension().unwrap().eq("xml") || path.extension().unwrap().eq("gz") {
        write_to_xml(population, path);
    } else {
        panic!("file format not supported. Either use `.xml`, `.xml.gz`, or `.binpb` as extension");
    }
}

fn load_from_xml(path: &Path, garage: &mut Garage) -> Population {
    let io_pop = IOPopulation::from_file(path.to_str().unwrap());
    create_ids(&io_pop, garage);
    create_population(&io_pop)
}

fn write_to_xml(_population: &Population, _path: &Path) {
    panic!("Write to xml is not implemented for Population. Only writing to `.binpb` is supported")
}

fn load_from_proto(path: &Path) -> Population {
    let wire_pop: crate::simulation::wire_types::population::Population = read_from_file(path);
    let persons = wire_pop
        .persons
        .into_iter()
        .map(|p| (Id::get(p.id), p))
        .collect();

    Population { persons }
}

fn write_to_proto(population: &Population, path: &Path) {
    info!("Converting Population into wire format");
    let persons: Vec<_> = population.persons.values().cloned().collect();
    let wire_pop = crate::simulation::wire_types::population::Population { persons };
    crate::simulation::io::proto::write_to_file(wire_pop, path);
}

fn create_ids(io_pop: &IOPopulation, garage: &mut Garage) {
    info!("Creating person ids.");
    // create person ids and collect strings for vehicle ids
    let raw_veh: Vec<_> = io_pop
        .persons
        .iter()
        .map(|p| Id::<Person>::create(p.id.as_str()))
        .flat_map(|p_id| {
            garage
                .vehicle_types
                .keys()
                .map(move |type_id| (p_id.clone(), type_id.clone()))
        })
        .collect();

    info!("Creating interaction activity types");
    // add interaction activity type for each vehicle type
    for (_, id) in raw_veh.iter() {
        Id::<String>::create(&format!("{} interaction", id.external()));
    }

    info!("Creating vehicle ids");
    for (person_id, type_id) in raw_veh {
        garage.add_veh_id(&person_id, &type_id);
    }

    info!("Creating activity types");
    // now iterate over all plans to extract activity ids
    io_pop
        .persons
        .iter()
        .flat_map(|person| person.plans.iter())
        .flat_map(|plan| plan.elements.iter())
        .filter_map(|element| match element {
            IOPlanElement::Activity(a) => Some(a),
            IOPlanElement::Leg(_) => None,
        })
        .map(|act| &act.r#type)
        .for_each(|act_type| {
            Id::<String>::create(act_type.as_str());
        });
}

fn create_population(io_pop: &IOPopulation) -> Population {
    let mut result = Population::new();
    for io_person in &io_pop.persons {
        let person = Person::from_io(io_person);
        result.persons.insert(Id::get(person.id()), person);
    }

    result
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct IORoute {
    pub r#type: String,
    pub start_link: String,
    pub end_link: String,
    pub trav_time: Option<String>,
    pub distance: f64,
    #[serde(rename = "vehicleRefId")]
    pub vehicle: Option<String>,

    // this needs to be parsed later
    #[serde(rename = "$value")]
    pub route: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct IOActivity {
    pub r#type: String,
    pub link: String,
    pub x: f64,
    pub y: f64,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub max_dur: Option<String>,
}

impl IOActivity {
    pub fn is_interaction(&self) -> bool {
        self.r#type.contains("interaction")
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct IOLeg {
    pub mode: String,
    pub dep_time: Option<String>,
    pub trav_time: Option<String>,
    pub route: IORoute,
    pub attributes: Option<Attrs>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum IOPlanElement {
    // the current matsim implementation has more logic with facility-id, link-id and coord.
    // This prototype assumes a fully specified activity with coord and link-id. We don't care about
    // Facilities at this stage.
    Activity(IOActivity),
    Leg(IOLeg),
}

impl IOPlanElement {
    pub fn get_activity(element: Option<&IOPlanElement>) -> Option<&IOActivity> {
        element.and_then(|e| {
            if let IOPlanElement::Activity(activity) = e {
                Some(activity)
            } else {
                None
            }
        })
    }

    pub fn get_leg(element: Option<&IOPlanElement>) -> Option<&IOLeg> {
        element.and_then(|e| {
            if let IOPlanElement::Leg(leg) = e {
                Some(leg)
            } else {
                None
            }
        })
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct IOPlan {
    pub selected: bool,
    // https://users.rust-lang.org/t/serde-deserializing-a-vector-of-enums/51647/2
    #[serde(rename = "$value")]
    pub elements: Vec<IOPlanElement>,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct IOPerson {
    pub id: String,
    #[serde(rename = "plan")]
    pub plans: Vec<IOPlan>,
}

impl IOPerson {
    pub fn selected_plan(&self) -> &IOPlan {
        self.plans.iter().find(|p| p.selected).unwrap()
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct IOPopulation {
    #[serde(rename = "person", default)]
    pub persons: Vec<IOPerson>,
}

impl IOPopulation {
    pub fn from_file(file_path: &str) -> IOPopulation {
        let population: IOPopulation = xml::read_from_file(file_path);
        info!(
            "IOPopulation: Finished reading population. Population contains {} persons",
            population.persons.len()
        );
        population
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use quick_xml::de::from_str;

    use crate::simulation::config::{MetisOptions, PartitionMethod};
    use crate::simulation::id::Id;
    use crate::simulation::network::global_network::Network;
    use crate::simulation::population::io::{load_from_xml, IOPlanElement, IOPopulation};
    use crate::simulation::vehicles::garage::Garage;

    /**
    This tests against the first person from the equil scenario. Probably this doesn't cover all
    possibilities and needs to improved later.
     */
    #[test]
    fn read_population_from_string() {
        let xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?>
<!DOCTYPE population SYSTEM \"http://www.matsim.org/files/dtd/population_v6.dtd\">

    <population>
        <attributes>
            <attribute name=\"coordinateReferenceSystem\" class=\"java.lang.String\">Atlantis</attribute>
        </attributes>

        <person id=\"1\">
            <attributes>
                <attribute name=\"vehicles\" class=\"org.matsim.vehicles.PersonVehicles\">{\"car\":\"1\"}</attribute>
            </attributes>
            <plan selected=\"yes\">
                <activity type=\"h\" link=\"1\" x=\"-25000.0\" y=\"0.0\" end_time=\"06:00:00\" >
                </activity>
                <leg mode=\"car\">
                    <attributes>
                        <attribute name=\"routingMode\" class=\"java.lang.String\">car</attribute>
                    </attributes>
                    <route type=\"links\" start_link=\"1\" end_link=\"20\" trav_time=\"undefined\" distance=\"25000.0\" vehicleRefId=\"null\">1 6 15 20</route>
                </leg>
                <activity type=\"w\" link=\"20\" x=\"10000.0\" y=\"0.0\" max_dur=\"00:10:00\" >
                </activity>
                <leg mode=\"car\">
                    <attributes>
                        <attribute name=\"routingMode\" class=\"java.lang.String\">car</attribute>
                    </attributes>
                    <route type=\"links\" start_link=\"20\" end_link=\"20\" trav_time=\"undefined\" distance=\"0.0\" vehicleRefId=\"null\">20</route>
                </leg>
                <activity type=\"w\" link=\"20\" x=\"10000.0\" y=\"0.0\" max_dur=\"03:30:00\" >
                </activity>
                <leg mode=\"car\">
                    <attributes>
                        <attribute name=\"routingMode\" class=\"java.lang.String\">car</attribute>
                    </attributes>
                    <route type=\"links\" start_link=\"20\" end_link=\"1\" trav_time=\"undefined\" distance=\"65000.0\" vehicleRefId=\"null\">20 21 22 23 1</route>
                </leg>
                <activity type=\"h\" link=\"1\" x=\"-25000.0\" y=\"0.0\" >
                </activity>
            </plan>
        </person>

    </population>";

        let population: IOPopulation = from_str(xml).unwrap();

        //test overall structure of population
        assert_eq!(1, population.persons.len());

        let person = population.persons.get(0).unwrap();
        assert_eq!("1", person.id);
        assert_eq!(1, person.plans.len());

        let plan = person.plans.get(0).unwrap();
        assert!(plan.selected);
        assert_eq!(7, plan.elements.len());

        // probe for first leg and second activity
        let leg1 = plan.elements.get(1).unwrap();
        match leg1 {
            IOPlanElement::Activity { .. } => {
                panic!("Plan Element at index 1 was expected to be a leg, but was Activity")
            }
            IOPlanElement::Leg(leg) => {
                // <leg mode=\"car\">
                //     <route type=\"links\" start_link=\"1\" end_link=\"20\" trav_time=\"undefined\" distance=\"25000.0\" vehicleRefId=\"null\">1 6 15 20</route>
                // </leg>
                assert_eq!("car", leg.mode);
                assert_eq!(None, leg.trav_time);
                assert_eq!(None, leg.dep_time);
                assert_eq!("links", leg.route.r#type);
                assert_eq!("1", leg.route.start_link);
                assert_eq!("20", leg.route.end_link);
                assert_eq!("undefined", leg.route.trav_time.as_ref().unwrap());
                assert_eq!(25000.0, leg.route.distance);
                assert_eq!("null", leg.route.vehicle.as_ref().unwrap());
                assert_eq!("1 6 15 20", leg.route.route.as_ref().unwrap())
            }
        }

        let activity2 = plan.elements.get(4).unwrap();
        match activity2 {
            IOPlanElement::Activity(activity) => {
                //<activity type=\"w\" link=\"20\" x=\"10000.0\" y=\"0.0\" max_dur=\"03:30:00\" >
                assert_eq!("w", activity.r#type);
                assert_eq!("20", activity.link);
                assert_eq!(10000.0, activity.x);
                assert_eq!(0.0, activity.y);
                assert_eq!(Some(String::from("03:30:00")), activity.max_dur);
                assert_eq!(None, activity.start_time);
                assert_eq!(None, activity.end_time);
            }
            IOPlanElement::Leg { .. } => {
                panic!("Plan element at inded 6 was expected to be an activity but was a Leg.")
            }
        }
    }

    #[test]
    fn read_example_file() {
        let population = IOPopulation::from_file("./assets/population-v6-34-persons.xml");
        assert_eq!(34, population.persons.len())
    }

    #[test]
    fn read_example_file_gzipped() {
        let population = IOPopulation::from_file("./assets/population-v6-34-persons.xml.gz");
        assert_eq!(34, population.persons.len())
    }

    #[test]
    fn test_conversion() {
        let _net = Network::from_file(
            "./assets/equil/equil-network.xml",
            2,
            PartitionMethod::Metis(MetisOptions::default()),
        );
        let mut garage = Garage::from_file(&PathBuf::from("./assets/equil/equil-vehicles.xml"));

        let pop = load_from_xml(
            &PathBuf::from("./assets/equil/equil-plans.xml.gz"),
            &mut garage,
        );
        assert_eq!(pop.persons.len(), 100);

        for i in 1u32..101 {
            assert!(pop
                .persons
                .get(&Id::get_from_ext(&format!("{}", i)))
                .is_some());
        }
    }
}

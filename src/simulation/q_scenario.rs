use crate::io::network::IONetwork;
use crate::io::population::IOPopulation;
use crate::simulation::q_network::QNetwork;
use crate::simulation::q_population::QPopulation;
use crate::simulation::q_vehicle::QVehicles;

pub struct QScenario<'a> {
    pub network: QNetwork<'a>,
    pub population: QPopulation,
    pub vehicles: QVehicles<'a>,
}

impl<'a> QScenario<'a> {
    pub fn from_container(network: &'a IONetwork, population: &'a IOPopulation) -> QScenario<'a> {
        let q_network = QNetwork::from_container(network);
        let mut q_vehicles = QVehicles::new();
        let q_population = QPopulation::from_container(population, &q_network, &mut q_vehicles);
        QScenario {
            network: q_network,
            population: q_population,
            vehicles: q_vehicles,
        }
    }
}

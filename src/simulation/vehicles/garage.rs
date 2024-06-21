use std::path::Path;

use nohash_hasher::IntMap;

use crate::simulation::id::Id;
use crate::simulation::vehicles::io::{from_file, to_file};
use crate::simulation::wire_types::messages::Vehicle;
use crate::simulation::wire_types::population::Person;
use crate::simulation::wire_types::vehicles::VehicleType;

#[derive(Debug)]
pub struct Garage {
    pub vehicles: IntMap<Id<Vehicle>, Id<VehicleType>>,
    pub vehicle_types: IntMap<Id<VehicleType>, VehicleType>,
}

#[derive(Debug)]
pub struct GarageVehicle {
    pub id: Id<Vehicle>,
    pub veh_type: Id<VehicleType>,
}

impl Default for Garage {
    fn default() -> Self {
        Garage::new()
    }
}

impl Garage {
    pub fn new() -> Self {
        Garage {
            vehicles: Default::default(),
            vehicle_types: Default::default(),
        }
    }

    pub fn from_file(file_path: &Path) -> Self {
        from_file(file_path)
    }

    pub fn to_file(&self, file_path: &Path) {
        to_file(self, file_path);
    }

    pub fn add_veh_type(&mut self, veh_type: VehicleType) {
        assert!(
            !self.vehicle_types.contains_key(&Id::get(veh_type.id)),
            "Vehicle type with id {:?} already exists.",
            Id::<VehicleType>::get(veh_type.id)
        );

        self.vehicle_types.insert(Id::get(veh_type.id), veh_type);
    }

    pub fn add_veh_id(&mut self, person_id: &Id<Person>, type_id: &Id<VehicleType>) -> Id<Vehicle> {
        let veh_id_ext = format!("{}_{}", person_id.external(), type_id.external());
        let veh_id = Id::create(&veh_id_ext);

        let veh_type = self.vehicle_types.get(type_id).unwrap();
        self.vehicles.insert(veh_id.clone(), Id::get(veh_type.id));

        veh_id
    }

    pub fn add_veh(&mut self, _veh_id: Id<Vehicle>, _veh_type_id: Id<VehicleType>) {
        panic!(
            "This method can only be used with chained modes. Which is currently not implemented"
        );
        /*
        let veh_type = self.vehicle_types.get(&veh_type_id).unwrap();
        match veh_type.lod {
            LevelOfDetail::Network => {
                let vehicle = GarageVehicle {
                    id: _veh_id.clone(),
                    veh_type: veh_type_id.clone(),
                };
                self.network_vehicles.insert(vehicle.id.clone(), vehicle);
            }
            LevelOfDetail::Teleported => {}
        }

         */
    }

    pub fn veh_id(&self, person_id: &Id<Person>, veh_type_id: &Id<VehicleType>) -> Id<Vehicle> {
        let external = format!("{}_{}", person_id.external(), veh_type_id.external());
        Id::get_from_ext(&external)
    }

    pub(crate) fn park_veh(&mut self, vehicle: Vehicle) -> Vec<Person> {
        /*let id = self.vehicle_ids.get(vehicle.id);
        let veh_type = self.vehicle_type_ids.get(vehicle.r#type);
        let garage_veh = GarageVehicle { id, veh_type };
        self.network_vehicles
            .insert(garage_veh.id.clone(), garage_veh);

         */

        // the above logic would park a vehicle within a garage. This only works if we have mass
        // conservation enabled. The scenario we're testing with doesn't. Therfore, we just take
        // the agents out of the vehicle and pretend we have parked the car.
        let mut agents = vehicle.passengers;
        agents.push(vehicle.driver.unwrap());
        agents
    }

    pub fn unpark_veh_with_passengers(
        &mut self,
        person: Person,
        passengers: Vec<Person>,
        id: &Id<Vehicle>,
    ) -> Vehicle {
        let veh_type_id = self
            .vehicles
            .get(id)
            .unwrap_or_else(|| panic!("Can't unpark vehicle with id {id}. It was not parked in this garage. Vehicle: {:?}", self.vehicles.len()));

        /*
        let veh_type_id = if let Some(veh_type_id) = self.vehicles.get(id) {
            veh_type_id.clone()
        } else if let Some(garage_veh) = self.network_vehicles.remove(id) {
            garage_veh.veh_type
        } else {
            panic!(
                "Can't unpark vehicle with id {}. It was not parked in this garage.",
                id.external()
            );
        };

         */

        // this method would fetch parked vehicles. But as we don't want to run with mass conservation
        // we just create vehicles on the fly.

        let veh_type = self.vehicle_types.get(veh_type_id).unwrap();

        Vehicle {
            id: id.internal(),
            curr_route_elem: 0,
            r#type: veh_type.id,
            max_v: veh_type.max_v,
            pce: veh_type.pce,
            driver: Some(person),
            passengers,
        }
    }

    pub fn unpark_veh(&mut self, person: Person, id: &Id<Vehicle>) -> Vehicle {
        self.unpark_veh_with_passengers(person, vec![], id)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::simulation::id::Id;
    use crate::simulation::vehicles::garage::Garage;
    use crate::test_utils::create_vehicle_type;

    #[test]
    fn add_veh_type() {
        let mut garage = Garage::new();
        let type_id = Id::create("some-type");
        let mode = Id::new_internal(0);
        let veh_type = create_vehicle_type(&type_id, mode);

        garage.add_veh_type(veh_type);

        assert_eq!(1, garage.vehicle_types.len());
    }

    #[test]
    #[should_panic]
    fn add_veh_type_reject_duplicate() {
        let mut garage = Garage::new();
        let type_id = Id::create("some-type");
        let mode = Id::new_internal(0);
        let veh_type1 = create_vehicle_type(&type_id, mode.clone());
        let veh_type2 = create_vehicle_type(&type_id, mode.clone());

        garage.add_veh_type(veh_type1);
        garage.add_veh_type(veh_type2);
    }

    #[test]
    fn from_file() {
        let garage = Garage::from_file(&PathBuf::from("./assets/3-links/vehicles.xml"));
        assert_eq!(3, garage.vehicle_types.len());
    }
}

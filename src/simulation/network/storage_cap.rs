/// StorageCap tracks changes in storage capacity for a link.
/// First of all it stores the maximum available storage capacity for a link.
/// Also, consumed and released capacity during a simulation time step is tracked
/// and can be queried separately. Once the time step is finished, the temporary
/// Bookkeeping can be applied to the overall consumed capacity by using the 'apply_updates'
/// method.
///
/// Consumed capacity can be queried immediately via 'currently_used', while released capacity
/// is treated separately. This is because we want vehicles which enter a link consume capacity
/// immediately, but capacity freed by vehicles leaving a link should only take effect in the next
/// simulation time step.
///
/// The consumed and released capacities are also tracked, so that we can figure out which
/// SplitInLinks must send storage capacity updates to upstream partitions. This logic
/// can be found in SimNetwork::move_links.
#[derive(Debug, Clone)]
pub struct StorageCap {
    max: f32,
    released: f32,
    consumed: f32,
    used: f32,
}

impl StorageCap {
    pub fn new(
        length: f64,
        perm_lanes: f32,
        capacity_h: f32,
        sample_size: f32,
        effective_cell_size: f32,
    ) -> Self {
        let flow_cap_s = capacity_h * sample_size / 3600.;
        let cap = length * perm_lanes as f64 * sample_size as f64 / effective_cell_size as f64;
        // storage capacity needs to be at least enough to handle the cap_per_time_step:
        let max_storage_cap = flow_cap_s.max(cap as f32);

        // the original code contains more logic to increase storage capacity for links with a low
        // free speed. Omit this for now, as we don't want to create a feature complete qsim

        Self {
            max: max_storage_cap,
            released: 0.0,
            consumed: 0.0,
            used: 0.0,
        }
    }

    pub fn currently_used(&self) -> f32 {
        self.used + self.consumed
    }

    pub fn released(&self) -> f32 {
        self.released
    }

    /// Consumes storage capacity on a link
    ///
    /// This method should be called when a vehicle enters a link.
    ///
    /// # Parameters
    /// * 'value' storage capacity to be consumed
    pub fn consume(&mut self, value: f32) {
        self.consumed += value;
    }

    /// Releases storage capacity on a link
    ///
    /// This method should be called when a vehicle leaves a link
    pub fn release(&mut self, value: f32) {
        self.released += value;
    }

    /// Applies consumed and released capacity during a simulated time step to the state of the storage capacity.
    /// Resets the released and consumed variables.
    pub fn apply_updates(&mut self) {
        self.used = 0f32.max(self.currently_used() - self.released);
        self.released = 0.0;
        self.consumed = 0.0;
    }

    /// Tests whether there is storage capacity available on the link.
    pub fn is_available(&self) -> bool {
        let available_cap = self.max - self.currently_used();
        available_cap > 0.0
    }
}

#[cfg(test)]
mod test {
    use crate::simulation::network::storage_cap::StorageCap;

    #[test]
    fn init_default() {
        let cap = StorageCap::new(100., 3., 1., 0.2, 7.5);
        assert_eq!(8., cap.max);
    }

    #[test]
    fn init_large_capacity() {
        let cap = StorageCap::new(100., 3., 360000., 0.2, 7.5);
        // we expect a storage size of 20. because it the flow cap/s is 20 (36000 * 0.2 / 3600)
        assert_eq!(20., cap.max);
    }
}

use crate::runtime::{RouteInspection, RouteUpdateRequest, RoutingRuntime, RoutingRuntimeError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedRouteUpdate {
    pub request: RouteUpdateRequest,
}

#[derive(Default)]
pub struct RecordingRoutingRuntime {
    pub updates: Vec<RecordedRouteUpdate>,
    pub inspections: Vec<RouteInspection>,
}

impl RecordingRoutingRuntime {
    pub fn with_inspections(inspections: Vec<RouteInspection>) -> Self {
        Self {
            updates: Vec::new(),
            inspections,
        }
    }
}

impl RoutingRuntime for RecordingRoutingRuntime {
    fn update_route(&mut self, request: RouteUpdateRequest) -> Result<(), RoutingRuntimeError> {
        self.updates.push(RecordedRouteUpdate { request });
        Ok(())
    }

    fn inspect_route(&mut self, _subtree_id: &str) -> Result<RouteInspection, RoutingRuntimeError> {
        if self.inspections.is_empty() {
            return Err(RoutingRuntimeError::InspectionFailed(
                "missing inspection response".into(),
            ));
        }
        Ok(self.inspections.remove(0))
    }
}

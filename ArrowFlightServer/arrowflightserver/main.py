import pyarrow as pa
import pyarrow.flight

class SimpleFlightServer(pa.flight.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)
        # Create a simple Arrow Table
        self.table = pa.table({
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
        # Ticket name to table reference
        self.datasets = {'people': self.table}
        self._location = location

    def do_get(self, context, ticket):
        """Return the table for the given ticket."""
        key = ticket.ticket.decode('utf-8')
        if key not in self.datasets:
            raise KeyError(f"Dataset {key} not found.")
        return pa.flight.RecordBatchStream(self.datasets[key])

    def list_flights(self, context, criteria):
        """Advertise our datasets."""
        for key in self.datasets:
            descriptor = pa.flight.FlightDescriptor.for_path(key)
            yield pa.flight.FlightInfo(
                self.datasets[key].schema,
                descriptor,
                endpoints=[
                    pa.flight.FlightEndpoint(ticket=pa.flight.Ticket(key), locations=[self._location])
                ],
                total_records=self.datasets[key].num_rows,
                total_bytes=0
            )

    def get_flight_info(self, context, descriptor):
        key = descriptor.path[0].decode('utf-8')
        if key not in self.datasets:
            raise KeyError(f"Dataset {key} not found.")
        return pa.flight.FlightInfo(
            self.datasets[key].schema,
            descriptor,
            endpoints=[
                pa.flight.FlightEndpoint(ticket=pa.flight.Ticket(key), locations=[self.location])
            ],
            total_records=self.datasets[key].num_rows,
            total_bytes=0
        )

if __name__ == "__main__":
    location = pa.flight.Location.for_grpc_tcp("0.0.0.0", 8815)
    server = SimpleFlightServer(location)
    print("Simple Apache Arrow Flight Server running at grpc://0.0.0.0:8815")
    server.serve()

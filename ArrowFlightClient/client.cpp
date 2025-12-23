#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <iostream>
#include <stdexcept>
#include <sstream>


inline void get_location(arrow::flight::Location & location) {
    auto status = arrow::flight::Location::Parse("grpc://host.lima.internal:8815");
    if (!status.ok()) {
        std::stringstream err;
        err << "Failed to parse server location: " << status.status() << std::endl;
        throw std::runtime_error(err.str());
    } else {
        location = status.ValueOrDie();
    }
}

inline void connect(std::unique_ptr<arrow::flight::FlightClient> & client, const arrow::flight::Location & location) {
    auto status = arrow::flight::FlightClient::Connect(location);
    if (!status.ok()) {
        std::stringstream err;
        err << "Failed to connect to Flight server: " << status.status() << std::endl;
        throw std::runtime_error(err.str());
    } else {
        client = std::move(status).ValueOrDie();
    }
}

inline void get_listing(const std::unique_ptr<arrow::flight::FlightClient> & client, std::unique_ptr<arrow::flight::FlightListing> & listing) {
    auto status = client->ListFlights();
    if (!status.ok()) {
        std::stringstream err;
        err << "ListFlights failed: " << status.status() << std::endl;
        throw std::runtime_error(err.str());
    } else {
        listing = std::move(status).ValueOrDie();
    }
}


inline bool get_next_flight_info(const std::unique_ptr<arrow::flight::FlightListing> & listing, std::unique_ptr<arrow::flight::FlightInfo> & info) {
    auto status = listing->Next();
    if (!status.ok()) {
        std::stringstream err;
        err << "ListFlights failed: " << status.status() << std::endl;
        throw std::runtime_error(err.str());
    }
    info = std::move(status).ValueOrDie();
    if (info->descriptor().type != arrow::flight::FlightDescriptor::PATH) return false;
    std::cout << "  - " << info->descriptor().path[0] << std::endl;
    return true;
}

inline void get_reader(const std::unique_ptr<arrow::flight::FlightClient> & client, const arrow::flight::Ticket & ticket, std::unique_ptr<arrow::flight::FlightStreamReader> & reader) {
    auto status = client->DoGet(ticket);
    if (!status.ok()) {
        std::stringstream err;
        err << "get_reader failed: " << status.status() << std::endl;
        throw std::runtime_error(err.str());
    } else {
        reader = std::move(status).ValueOrDie();
    }
}


int client_get() {
    try {

        arrow::flight::Location location;
        get_location(location);

        // Connect to server at localhost:8815
        std::unique_ptr<arrow::flight::FlightClient> client;
        connect(client, location);

        // List available flights
        std::unique_ptr<arrow::flight::FlightListing> listing;
        get_listing(client, listing);

        std::cout << "Available flights:\n";
        std::unique_ptr<arrow::flight::FlightInfo> info;
        while (true) {
            if (get_next_flight_info(listing, info)) break;
        }

        // Fetch the "people" dataset
        arrow::flight::Ticket ticket{"people"};
        std::unique_ptr<arrow::flight::FlightStreamReader> reader;

        get_reader(client, ticket, reader);


        // Print received table

        std::cout << "\nResults:\n" << std::endl;
        while (true) {
            auto status_reader_next = reader->Next();
            if (!status_reader_next.ok()) break;
            arrow::flight::FlightStreamChunk next_chunk = status_reader_next.ValueOrDie();
            if (!next_chunk.data /*&&  !next_chunk.app_metadata*/) break;
            std::shared_ptr<arrow::RecordBatch> batch = next_chunk.data;
            // Print each row
            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                for (int j = 0; j < batch->num_columns(); ++j) {
                    auto col = batch->column(j);
                    std::string value = col->ToString();
                    std::cout << batch->column_name(j) << ": " << col->ToString() << "  ";
                }
                std::cout << std::endl;
            }
        }

    } catch (std::runtime_error & err) {
        std::cerr << err.what();
        return 1;
    }
    return 0;
}


arrow::Status client_put() {

        arrow::flight::Location location;
        ARROW_ASSIGN_OR_RAISE(location, arrow::flight::Location::Parse("grpc://host.lima.internal:8815"));

        // Connect to server at localhost:8815
        std::unique_ptr<arrow::flight::FlightClient> client;
        ARROW_ASSIGN_OR_RAISE(client, arrow::flight::FlightClient::Connect(location));

        auto schema = arrow::schema({arrow::field("a", arrow::int32())});

        // Create data for batch (say 5 rows: 0, 1, 2, 3, 4)
        std::shared_ptr<arrow::Array> array;
        arrow::Int32Builder builder;
        for (int i = 0; i < 5; ++i) ARROW_RETURN_NOT_OK(builder.Append(i));
        ARROW_ASSIGN_OR_RAISE(array, builder.Finish());
        auto batch = arrow::RecordBatch::Make(schema, 5, {array});

        // Prepare DoPut call (Descriptor can be empty for this test)
        arrow::flight::FlightDescriptor descriptor = arrow::flight::FlightDescriptor::Path({"example"});

        // Setup writer and reader for DoPut
        arrow::flight::FlightClient::DoPutResult put_result;
        ARROW_ASSIGN_OR_RAISE(put_result, client->DoPut(descriptor, schema));

        // Send the batch
        ARROW_RETURN_NOT_OK(put_result.writer->WriteRecordBatch(*batch));
        ARROW_RETURN_NOT_OK(put_result.writer->DoneWriting());

        std::cout << "Record batch sent!" << std::endl;

        return arrow::Status::OK();
}


int main() {
    auto status = client_put();
    if (!status.ok()) {
        std::cerr << "client_put failed with: " << status << std::endl;
        return 1;
    }
}

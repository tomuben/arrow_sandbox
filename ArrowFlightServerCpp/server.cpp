#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/io/api.h>
#include <iostream>
#include <Python.h>
#include <arrow/python/pyarrow.h>
#include <fstream>
#include <sstream>



using namespace arrow;
using namespace arrow::flight;

int call_python_code(const std::shared_ptr<arrow::RecordBatch> &batch);


class SimpleFlightServer : public FlightServerBase {
public:
    Status DoPut(const ServerCallContext &context, std::unique_ptr<FlightMessageReader> reader, std::unique_ptr<FlightMetadataWriter> writer) override {

        std::shared_ptr<Schema> schema;
        ARROW_ASSIGN_OR_RAISE(schema, reader->GetSchema());

        std::cout << "Received DoPut call with schema: " << schema->ToString() << std::endl;

        while (true) {
            FlightStreamChunk next_chunk;
            ARROW_ASSIGN_OR_RAISE(next_chunk, reader->Next());
            if (!next_chunk.data) {
                // No more batches
                break;
            }
            std::cout << "Calling python for batch with " << next_chunk.data->num_rows() << " rows." << std::endl;
            call_python_code(next_chunk.data);
            std::cout << "Finished with python." << std::endl;
        }
        std::cout << "DoPut completed." << std::endl;
        return Status::OK();
    }
};

int call_python_code(const std::shared_ptr<arrow::RecordBatch> &batch) {
    // 2. Define the path to your external Python file
    PyGILState_STATE gstate = PyGILState_Ensure();
    const char* filename = "action.py";

    std::cout << "Test 1 " << std::endl;
    // 3. Read the contents of the file into a string
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Error: Could not open file " << filename << std::endl;
        return 1;
    }
    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string code_string = buffer.str();

    std::cout << "Test 2 " << std::endl;
    std::cout << code_string << std::endl;

    PyObject *code = Py_CompileString(code_string.c_str(), "action.py", Py_file_input);
    if (code == NULL) {
        std::cerr << "Unable to compile action.py" << std::endl;
        PyErr_Print();
        return 1;
    }

    std::cout << "Test 3 " << std::endl;

    PyObject* module = PyImport_ExecCodeModule((char*)"action", code);

    if (module == nullptr) {
        std::cerr << "Error: Could not execute Python module" << std::endl;
        PyErr_Print();
        Py_DECREF(code);
        return 1;
    }


    std::cout << "Test 4 " << std::endl;

    PyObject *pFunc = PyObject_GetAttrString(module, "do_something_with_arrow");
    if (!pFunc || !PyCallable_Check(pFunc)) {
        std::cerr << "Unable to get do_something_with_arrow";
        PyErr_Print();
        Py_XDECREF(code);
        Py_XDECREF(module);
        return 1;
    }

    std::cout << "Wrapping record batch..." << std::endl;

    PyObject * py_arrow_batch = arrow::py::wrap_batch(batch);
    if (py_arrow_batch == nullptr) {
        std::cerr << "Unable to wrap arrow batch";
        PyErr_Print();
        Py_XDECREF(code);
        Py_XDECREF(module);
        Py_XDECREF(pFunc);
        return 1;
    }

    PyObject *pArgs = PyTuple_New(1); // function takes 1 argument
    PyTuple_SetItem(pArgs, 0, py_arrow_batch); // Steals

    std::cout << "Now calling the python function..." << std::endl;

    PyObject *pValue = PyObject_CallObject(pFunc, pArgs);
    if (pValue != nullptr) {
        // process result
        Py_DECREF(pValue);
    } else {
        std::cerr << "Unable to call do_something_with_arrow";
        PyErr_Print();
        Py_XDECREF(code);
        Py_XDECREF(pArgs);
        Py_XDECREF(module);
        Py_XDECREF(pFunc);
        return 1;
    }

    std::cout << "Finished.." << std::endl;

    Py_XDECREF(pValue);
    Py_XDECREF(code);
    Py_XDECREF(pArgs);
    Py_XDECREF(pFunc);
    Py_XDECREF(module); // PyImport_ExecCodeModule returns a new reference

    std::cout << "Executed successfully the python code" << std::endl;
    PyGILState_Release(gstate);
    return 0;
}

arrow::Status build_array(std::vector<std::shared_ptr<arrow::Array>> & columns) {
    arrow::Int32Builder int_builder;
    std::shared_ptr<arrow::Array> int_array;
    ARROW_RETURN_NOT_OK(int_builder.AppendValues({1, 2, 3}));
    ARROW_ASSIGN_OR_RAISE(int_array, int_builder.Finish());
    columns.push_back(int_array);
    return arrow::Status::OK();
}
int main(int argc, char** argv) {
    Py_Initialize();
    PyEval_InitThreads();

    if (0 != arrow::py::import_pyarrow()) {
        std::cerr << "Error initializing pyarrow" << std::endl;
        return 1;
    }
    PyEval_SaveThread();
//    auto schema = arrow::schema({
//        arrow::field("int_col", arrow::int32())
//    });
//    std::shared_ptr<arrow::Array> int_array;
//    int num_rows = 3;
//    std::vector<std::shared_ptr<arrow::Array>> columns;
//    auto status_array = build_array(columns);
//    if (!status_array.ok()) {
//        std::cerr << "Error building array" << status_array << std::endl;
//        return 1;
//    }
//
//    std::shared_ptr<arrow::RecordBatch> record_batch =
//        arrow::RecordBatch::Make(schema, num_rows, std::move(columns));
//    call_python_code(record_batch);


    SimpleFlightServer server;

    arrow::flight::Location location;
    auto status_location = arrow::flight::Location::ForGrpcTcp("0.0.0.0", 8815);
    if (!status_location.ok()) {
        std::cerr << "Could not parse location:" << status_location.status() << std::endl;
        return 1;
    }
    location = status_location.ValueOrDie();

    arrow::flight::FlightServerOptions options(location);

    std::cout << "Starting Flight server on " << location.ToString() << std::endl;
    arrow::Status status = server.Init(options);
    if (!status.ok()) {
        std::cerr << "Failed to start server: " << status << std::endl;
        return 1;
    }
    // Run until exit
    arrow::Status serve_status = server.Serve();
    if (!serve_status.ok()) {
        std::cerr << "Failed to run server: " << serve_status << std::endl;
        return 1;
    }
    return 0;
}

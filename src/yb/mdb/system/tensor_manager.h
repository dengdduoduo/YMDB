#pragma once

#include <cstdint>
#include <stdexcept>

#include "graph_models/common/datatypes/tensor/tensor.h"
#include "graph_models/object_id.h"

class TensorManager {
public:
    static TensorManager& get_manager() {
        static TensorManager instance;
        return instance;
    }

    uint64_t get_id(const char* bytes, std::size_t num_bytes) {
        return ObjectId::MASK_NOT_FOUND;
    }

    template<typename T>
    tensor::Tensor<T> get_tensor(ObjectId tensor_oid) {
        throw std::runtime_error("TensorManager::get_tensor not implemented");
    }

    static void init(uint64_t, uint64_t) { }

private:
    TensorManager() = default;
};

inline extern TensorManager& tensor_manager = TensorManager::get_manager();

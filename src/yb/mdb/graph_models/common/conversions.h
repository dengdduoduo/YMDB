#pragma once
#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "graph_models/common/datatypes/datetime.h"
#include "graph_models/common/datatypes/decimal.h"
#include "graph_models/common/datatypes/tensor/tensor.h"
#include "graph_models/object_id.h"

std::string ObjectId_to_string(ObjectId);
std::string get_iri(ObjectId);

namespace Common {
namespace Conversions {

namespace {

inline uint64_t fnv1a_64(const char* data, size_t len)
{
    constexpr uint64_t FNV_OFFSET_BASIS = 14695981039346656037ULL;
    constexpr uint64_t FNV_PRIME = 1099511628211ULL;
    uint64_t hash = FNV_OFFSET_BASIS;
    for (size_t i = 0; i < len; i++) {
        hash ^= static_cast<unsigned char>(data[i]);
        hash *= FNV_PRIME;
    }
    return hash;
}

inline std::unordered_map<uint64_t, std::vector<char>>& extern_tensor_storage()
{
    static std::unordered_map<uint64_t, std::vector<char>> storage;
    return storage;
}

inline std::mutex& tensor_storage_mtx()
{
    static std::mutex mtx;
    return mtx;
}

} // anonymous namespace

inline ObjectId pack_tensor(const tensor::Tensor<float>& t)
{
    if (t.empty()) {
        return ObjectId(tensor::Tensor<float>::get_inline_mask());
    }
    const auto* bytes = reinterpret_cast<const char*>(t.data());
    const auto num_bytes = sizeof(float) * t.size();
    uint64_t hash = fnv1a_64(bytes, num_bytes);
    uint64_t content = hash & ObjectId::MASK_EXTERNAL_ID;
    {
        std::lock_guard<std::mutex> lock(tensor_storage_mtx());
        auto& storage = extern_tensor_storage();
        if (storage.find(content) == storage.end()) {
            storage[content].assign(bytes, bytes + num_bytes);
        }
    }
    return ObjectId(tensor::Tensor<float>::get_external_mask() | content);
}

inline ObjectId pack_tensor(const tensor::Tensor<double>& t)
{
    if (t.empty()) {
        return ObjectId(tensor::Tensor<double>::get_inline_mask());
    }
    const auto* bytes = reinterpret_cast<const char*>(t.data());
    const auto num_bytes = sizeof(double) * t.size();
    uint64_t hash = fnv1a_64(bytes, num_bytes);
    uint64_t content = hash & ObjectId::MASK_EXTERNAL_ID;
    {
        std::lock_guard<std::mutex> lock(tensor_storage_mtx());
        auto& storage = extern_tensor_storage();
        if (storage.find(content) == storage.end()) {
            storage[content].assign(bytes, bytes + num_bytes);
        }
    }
    return ObjectId(tensor::Tensor<double>::get_external_mask() | content);
}

template<typename T>
inline ObjectId pack_tensor(const tensor::Tensor<T>& tensor)
{
    if (tensor.empty()) {
        return ObjectId(tensor::Tensor<T>::get_inline_mask());
    }
    const auto* bytes = reinterpret_cast<const char*>(tensor.data());
    const auto num_bytes = sizeof(T) * tensor.size();
    uint64_t hash = fnv1a_64(bytes, num_bytes);
    uint64_t content = hash & ObjectId::MASK_EXTERNAL_ID;
    {
        std::lock_guard<std::mutex> lock(tensor_storage_mtx());
        auto& storage = extern_tensor_storage();
        if (storage.find(content) == storage.end()) {
            storage[content].assign(bytes, bytes + num_bytes);
        }
    }
    return ObjectId(tensor::Tensor<T>::get_external_mask() | content);
}

template<typename T>
inline tensor::Tensor<T> unpack_tensor(ObjectId oid)
{
    if (oid.get_sub_type() != tensor::Tensor<T>::get_subtype()) {
        return tensor::Tensor<T>();
    }
    if (oid.get_mod() == ObjectId::MOD_INLINE) {
        return tensor::Tensor<T>();
    }
    uint64_t external_id = oid.id & ObjectId::MASK_EXTERNAL_ID;
    {
        std::lock_guard<std::mutex> lock(tensor_storage_mtx());
        auto& storage = extern_tensor_storage();
        auto it = storage.find(external_id);
        if (it != storage.end()) {
            const auto& vec = it->second;
            return tensor::Tensor<T>::from_bytes(vec.data(), vec.size());
        }
    }
    return tensor::Tensor<T>();
}

template<typename T>
inline tensor::Tensor<T> to_tensor(ObjectId oid)
{
    return unpack_tensor<T>(oid);
}
} // namespace Conversions
} // namespace Common

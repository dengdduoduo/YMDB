#pragma once

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <string>

#include "graph_models/object_id.h"
#include "operations/operations.h"
#include "tensor_allocator.h"

namespace tensor {

template<typename T>
class Tensor {
    static_assert(std::is_floating_point_v<T>, "instantiation of Tensor for non-floating-point types"
                                               " is not allowed.");

public:
    using size_type = typename std::size_t;
    using comparison = typename operations::comparison<T>;

    Tensor() :
        size_(0),
        data_(nullptr) { }

    explicit Tensor(size_type n) :
        size_(n),
        data_(nullptr)
    {
        assert(n > 0);
        data_ = allocator::allocate(n);
    }

    Tensor(const T* ptr, size_type n) :
        size_(n),
        data_(nullptr)
    {
        assert(n > 0);
        data_ = allocator::allocate(n);
        std::memcpy(data_, ptr, n * sizeof(T));
    }

    Tensor(std::initializer_list<T> list) :
        size_(list.size()),
        data_(nullptr)
    {
        assert(list.size() > 0);
        data_ = allocator::allocate(list.size());
        std::memcpy(data_, std::data(list), sizeof(T) * list.size());
    }

    Tensor(const Tensor& other) :
        size_(other.size_),
        data_(nullptr)
    {
        if (size_ > 0) {
            data_ = allocator::allocate(size_);
            std::memcpy(data_, other.data_, size_ * sizeof(T));
        }
    }

    Tensor(Tensor&& other) noexcept :
        size_(other.size_),
        data_(other.data_)
    {
        other.size_ = 0;
        other.data_ = nullptr;
    }

    ~Tensor()
    {
        if (size_ > 0) {
            allocator::deallocate(data_);
        }
    }

    Tensor& operator=(const Tensor& rhs)
    {
        if (this == &rhs) {
            return *this;
        }

        if (size_ > 0) {
            allocator::deallocate(data_);
            data_ = nullptr;
        }

        size_ = rhs.size_;

        if (size_ > 0) {
            data_ = allocator::allocate(size_);
            std::memcpy(data_, rhs.data_, size_ * sizeof(T));
        }

        return *this;
    }

    Tensor& operator=(Tensor&& rhs) noexcept
    {
        if (this == &rhs) {
            return *this;
        }

        if (size_ > 0) {
            allocator::deallocate(data_);
        }

        size_ = rhs.size_;
        data_ = rhs.data_;
        rhs.size_ = 0;
        rhs.data_ = nullptr;

        return *this;
    }

    static Tensor from_bytes(const char* bytes, size_type num_bytes)
    {
        size_type n = num_bytes / sizeof(T);
        if (n == 0) {
            return Tensor();
        }
        Tensor result(n);
        std::memcpy(result.data_, bytes, num_bytes);
        return result;
    }

    bool operator==(const Tensor& rhs) const noexcept
    {
        return comparison::eq(data_, rhs.data_, size_, rhs.size_);
    }

    bool operator!=(const Tensor& rhs) const noexcept
    {
        return comparison::neq(data_, rhs.data_, size_, rhs.size_);
    }

    bool operator<(const Tensor& rhs) const noexcept
    {
        return comparison::lt(data_, rhs.data_, size_, rhs.size_);
    }

    bool operator>(const Tensor& rhs) const noexcept
    {
        return comparison::gt(data_, rhs.data_, size_, rhs.size_);
    }

    bool operator<=(const Tensor& rhs) const noexcept
    {
        return comparison::leq(data_, rhs.data_, size_, rhs.size_);
    }

    bool operator>=(const Tensor& rhs) const noexcept
    {
        return comparison::geq(data_, rhs.data_, size_, rhs.size_);
    }

    bool empty() const noexcept
    {
        return size_ == 0;
    }

    size_type size() const noexcept
    {
        return size_;
    }

    const T* data() const noexcept
    {
        return data_;
    }

    T& operator[](size_type pos) noexcept
    {
        assert(pos < size_);
        return data_[pos];
    }

    const T& operator[](size_type pos) const noexcept
    {
        assert(pos < size_);
        return data_[pos];
    }

    T sum() const noexcept
    {
        if (empty()) {
            return T { 0.0 };
        }
        return operations::computation<T>::sum(data_, size_);
    }

    T cosine_similarity(const Tensor& other) const noexcept
    {
        if (empty() || other.empty()) {
            return T { 0.0 };
        }
        assert(size_ == other.size_);
        return operations::computation<T>::cosine_similarity(data_, other.data_, size_);
    }

    T euclidean_distance(const Tensor& other) const noexcept
    {
        if (empty() || other.empty()) {
            return T { 0.0 };
        }
        assert(size_ == other.size_);
        return operations::computation<T>::euclidean_distance(data_, other.data_, size_);
    }

    T manhattan_distance(const Tensor& other) const noexcept
    {
        if (empty() || other.empty()) {
            return T { 0.0 };
        }
        assert(size_ == other.size_);
        return operations::computation<T>::manhattan_distance(data_, other.data_, size_);
    }

    T dot(const Tensor& other) const noexcept
    {
        if (empty() || other.empty()) {
            return T { 0.0 };
        }
        assert(size_ == other.size_);
        return operations::computation<T>::dot(data_, other.data_, size_);
    }

    T cosine_distance(const Tensor& other) const noexcept
    {
        if (empty() || other.empty()) {
            return T { 0.0 };
        }
        assert(size_ == other.size_);
        return T { 1.0 } - cosine_similarity(other);
    }

    Tensor abs() const noexcept
    {
        Tensor res(*this);
        if (!empty()) {
            operations::modifiers<T>::abs(res.data_, res.size_);
        }
        return res;
    }

    Tensor ceil() const noexcept
    {
        Tensor res(*this);
        if (!empty()) {
            operations::modifiers<T>::ceil(res.data_, res.size_);
        }
        return res;
    }

    Tensor floor() const noexcept
    {
        Tensor res(*this);
        if (!empty()) {
            operations::modifiers<T>::floor(res.data_, res.size_);
        }
        return res;
    }

    Tensor operator-() const noexcept
    {
        Tensor res(*this);
        if (!empty()) {
            operations::modifiers<T>::neg(res.data_, res.size_);
        }
        return res;
    }

    Tensor inverse() const noexcept
    {
        Tensor res(*this);
        if (!empty()) {
            operations::modifiers<T>::inverse(res.data_, res.size_);
        }
        return res;
    }

    Tensor sqrt() const noexcept
    {
        Tensor res(*this);
        if (!empty()) {
            operations::modifiers<T>::sqrt(res.data_, res.size_);
        }
        return res;
    }

    Tensor rounding_sparql11() const noexcept
    {
        Tensor res(*this);
        if (!empty()) {
            operations::modifiers<T>::sparql11_round(res.data_, res.size_);
        }
        return res;
    }

    void sparql11_round() noexcept
    {
        if (!empty()) {
            operations::modifiers<T>::sparql11_round(data_, size_);
        }
    }

    void negate() noexcept
    {
        if (!empty()) {
            operations::modifiers<T>::neg(data_, size_);
        }
    }

    void inplace_abs() noexcept
    {
        if (!empty()) {
            operations::modifiers<T>::abs(data_, size_);
        }
    }

    void inplace_ceil() noexcept
    {
        if (!empty()) {
            operations::modifiers<T>::ceil(data_, size_);
        }
    }

    void inplace_floor() noexcept
    {
        if (!empty()) {
            operations::modifiers<T>::floor(data_, size_);
        }
    }

    void inplace_inverse() noexcept
    {
        if (!empty()) {
            operations::modifiers<T>::inverse(data_, size_);
        }
    }

    void inplace_sqrt() noexcept
    {
        if (!empty()) {
            operations::modifiers<T>::sqrt(data_, size_);
        }
    }

    static Tensor from_literal(const std::string& str, bool* has_error)
    {
        *has_error = false;

        std::string trimmed = str;
        size_t start = trimmed.find_first_not_of(" \t\n\r");
        if (start == std::string::npos || trimmed[start] != '[') {
            *has_error = true;
            return Tensor();
        }
        size_t end = trimmed.find_last_not_of(" \t\n\r");
        if (end == std::string::npos || trimmed[end] != ']') {
            *has_error = true;
            return Tensor();
        }

        std::string inner = trimmed.substr(start + 1, end - start - 1);
        if (inner.empty()) {
            return Tensor();
        }

        std::vector<T> values;
        size_t pos = 0;
        while (pos < inner.size()) {
            pos = inner.find_first_not_of(" \t\n\r", pos);
            if (pos == std::string::npos) break;

            size_t num_end = pos;
            while (num_end < inner.size() && inner[num_end] != ',' && inner[num_end] != ' '
                   && inner[num_end] != '\t' && inner[num_end] != '\n' && inner[num_end] != '\r')
            {
                num_end++;
            }

            std::string num_str = inner.substr(pos, num_end - pos);
            if (num_str.empty()) {
                pos = num_end + 1;
                continue;
            }

            try {
                if constexpr (std::is_same_v<T, float>) {
                    values.push_back(std::stof(num_str));
                } else {
                    values.push_back(std::stod(num_str));
                }
            } catch (...) {
                *has_error = true;
                return Tensor();
            }

            pos = inner.find(',', num_end);
            if (pos != std::string::npos) {
                pos++;
            } else {
                break;
            }
        }

        if (values.empty()) {
            return Tensor();
        }

        Tensor result(values.size());
        std::copy(values.begin(), values.end(), result.data_);
        return result;
    }

    void inplace_add_tensor(const Tensor& other) noexcept
    {
        assert(size_ == other.size_);
        if (empty()) return;
        operations::vectorial_arithmetic<T>::add(data_, other.data_, data_, size_);
    }

    void inplace_add_scalar(T scalar) noexcept
    {
        if (empty()) return;
        operations::scalar_arithmetic<T>::add(data_, scalar, data_, size_);
    }

    void inplace_sub_tensor(const Tensor& other) noexcept
    {
        assert(size_ == other.size_);
        if (empty()) return;
        operations::vectorial_arithmetic<T>::sub(data_, other.data_, data_, size_);
    }

    void inplace_sub_scalar_rhs(T scalar) noexcept
    {
        if (empty()) return;
        operations::scalar_arithmetic<T>::sub(data_, scalar, data_, size_);
    }

    void inplace_sub_scalar_lhs(T scalar) noexcept
    {
        if (empty()) return;
        operations::scalar_arithmetic<T>::sub_lhs(scalar, data_, data_, size_);
    }

    void inplace_mul_tensor(const Tensor& other) noexcept
    {
        assert(size_ == other.size_);
        if (empty()) return;
        operations::vectorial_arithmetic<T>::mul(data_, other.data_, data_, size_);
    }

    void inplace_mul_scalar(T scalar) noexcept
    {
        if (empty()) return;
        operations::scalar_arithmetic<T>::mul(data_, scalar, data_, size_);
    }

    void inplace_div_tensor(const Tensor& other) noexcept
    {
        assert(size_ == other.size_);
        if (empty()) return;
        operations::vectorial_arithmetic<T>::div(data_, other.data_, data_, size_);
    }

    void inplace_div_scalar_rhs(T scalar) noexcept
    {
        if (empty()) return;
        operations::scalar_arithmetic<T>::div(data_, scalar, data_, size_);
    }

    void inplace_div_scalar_lhs(T scalar) noexcept
    {
        if (empty()) return;
        operations::scalar_arithmetic<T>::div_lhs(scalar, data_, data_, size_);
    }

    void pow(const Tensor& exponent) noexcept
    {
        assert(size_ == exponent.size_);
        if (empty()) return;
        for (size_type i = 0; i < size_; i++) {
            data_[i] = std::pow(data_[i], exponent.data_[i]);
        }
    }

    void pow(T exponent) noexcept
    {
        if (empty()) return;
        operations::scalar_arithmetic<T>::pow(data_, exponent, data_, size_);
    }

    static constexpr uint64_t get_subtype() noexcept
    {
        if constexpr (std::is_same_v<T, float>) {
            return ObjectId::MASK_TENSOR_FLOAT;
        } else {
            return ObjectId::MASK_TENSOR_DOUBLE;
        }
    }

    static constexpr uint64_t get_inline_mask() noexcept
    {
        if constexpr (std::is_same_v<T, float>) {
            return ObjectId::MASK_TENSOR_FLOAT_INLINED;
        } else {
            return ObjectId::MASK_TENSOR_DOUBLE_INLINED;
        }
    }

    static constexpr uint64_t get_external_mask() noexcept
    {
        if constexpr (std::is_same_v<T, float>) {
            return ObjectId::MASK_TENSOR_FLOAT_EXTERN;
        } else {
            return ObjectId::MASK_TENSOR_DOUBLE_EXTERN;
        }
    }

    static constexpr uint64_t get_tmp_mask() noexcept
    {
        if constexpr (std::is_same_v<T, float>) {
            return ObjectId::MASK_TENSOR_FLOAT_TMP;
        } else {
            return ObjectId::MASK_TENSOR_DOUBLE_TMP;
        }
    }

    std::string to_string() const noexcept
    {
        if (empty()) {
            return "[]";
        }
        std::string res = "[";
        for (size_type i = 0; i < size_; i++) {
            if (i > 0) {
                res += ", ";
            }
            res += std::to_string(data_[i]);
        }
        res += "]";
        return res;
    }

private:
    using allocator = aligned_allocator<T, operations::alignment_bytes>;

    size_type size_;
    T* data_;
};

inline std::ostream& operator<<(std::ostream& os, const Tensor<float>& tensor)
{
    return os << tensor.to_string();
}

inline std::ostream& operator<<(std::ostream& os, const Tensor<double>& tensor)
{
    return os << tensor.to_string();
}

} // namespace tensor

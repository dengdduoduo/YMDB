#pragma once

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>
#include <ostream>
#include <sstream>
#include <string>
#include <stdexcept>

// ANTLR undefines EOF; Boost's stream helpers still require the C macro.
#pragma push_macro("EOF")
#ifndef EOF
#define EOF (-1)
#endif
#include <boost/multiprecision/cpp_int.hpp>
#pragma pop_macro("EOF")

#include "thirdparty/dragonbox/dragonbox.h"

class Decimal {
private:
    using BigInt = boost::multiprecision::cpp_int;

    uint64_t significand;
    int8_t exponent;
    bool negative;

    constexpr Decimal(uint64_t significand, int8_t exponent, bool negative) :
        significand(significand),
        exponent(exponent),
        negative(negative)
    { }

public:
    static constexpr size_t MAX_DIGIT_PRECISION = 19;
    static constexpr auto EXTERN_BUFFER_SIZE = 10;

    // Initializes a zero
    constexpr Decimal() :
        significand(0),
        exponent(0),
        negative(false)
    { }

    explicit Decimal(int64_t i) :
        significand(i >= 0 ? static_cast<uint64_t>(i) : uint64_t{0} - static_cast<uint64_t>(i)),
        exponent(0),
        negative(i < 0)
    {
        normalize();
    }

    explicit Decimal(float f) : Decimal() {
        if (f == 0) return;
        if (!std::isnormal(f)) {
            throw std::out_of_range("Floating value is outside decimal range");
        }
        const auto value = jkj::dragonbox::to_decimal(f);
        BigInt coefficient = value.significand;
        if (value.is_negative) coefficient = -coefficient;
        *this = from_exact(coefficient, value.exponent);
    }

    explicit Decimal(double d) : Decimal() {
        if (d == 0) return;
        if (!std::isnormal(d)) {
            throw std::out_of_range("Floating value is outside decimal range");
        }
        const auto value = jkj::dragonbox::to_decimal(d);
        BigInt coefficient = value.significand;
        if (value.is_negative) coefficient = -coefficient;
        *this = from_exact(coefficient, value.exponent);
    }

    Decimal(std::string_view sv, bool* error) : Decimal() {
        *error = true;
        if (sv.empty()) return;
        bool sign = false;
        if (sv.front() == '+' || sv.front() == '-') {
            sign = sv.front() == '-';
            sv.remove_prefix(1);
        }
        if (sv.empty()) return;
        std::string digits;
        bool point = false;
        size_t fractional = 0;
        for (char c : sv) {
            if (c == '.' && !point) {
                point = true;
            } else if (c >= '0' && c <= '9') {
                digits += c;
                if (point) ++fractional;
            } else {
                return;
            }
        }
        if (digits.empty()) return;
        const auto first = digits.find_first_not_of('0');
        if (first == std::string::npos) {
            *error = false;
            return;
        }
        const auto last = digits.find_last_not_of('0');
        if (last - first + 1 > MAX_DIGIT_PRECISION) return;
        const auto trailing = digits.size() - last - 1;
        if (fractional > trailing + 128 || trailing > fractional + 127) return;
        const int exp = trailing >= fractional ? static_cast<int>(trailing - fractional) :
                                                -static_cast<int>(fractional - trailing);
        uint64_t coefficient = 0;
        for (size_t i = first; i <= last; ++i) coefficient = coefficient * 10 + (digits[i] - '0');
        significand = coefficient;
        exponent = static_cast<int8_t>(exp);
        negative = sign;
        *error = false;
    }

    static Decimal from_external(const char* buffer)
    {
        auto bytes = reinterpret_cast<const uint8_t*>(buffer);

        uint64_t significand = 0;
        for (int i = 0, shift = 0; i < 8; ++i, shift += 8) {
            significand |= static_cast<uint64_t>(bytes[i]) << shift;
        }

        auto res_significand = significand;
        auto res_exponent = reinterpret_cast<const int8_t*>(bytes)[8];
        auto res_negative = bytes[9] == 1;

        return Decimal(res_significand, res_exponent, res_negative);
    }

    void serialize_extern(char* buffer) const
    {
        auto bytes = reinterpret_cast<uint8_t*>(buffer);

        for (size_t i = 0, shift = 0; i < 8; ++i, shift += 8) {
            bytes[i] = (significand >> shift) & 0xFF;
        }

        reinterpret_cast<int8_t*>(bytes)[8] = exponent;
        bytes[9] = negative ? 1 : 0;
    }

    static Decimal from_inlined(uint64_t i)
    {
        auto bytes = reinterpret_cast<uint8_t*>(&i);

        uint64_t significand = 0;
        significand += static_cast<uint64_t>(bytes[0]);
        significand += static_cast<uint64_t>(bytes[1]) << 8;
        significand += static_cast<uint64_t>(bytes[2]) << 16;
        significand += static_cast<uint64_t>(bytes[3]) << 24;
        significand += static_cast<uint64_t>(bytes[4]) << 32;
        significand += (static_cast<uint64_t>(bytes[5]) & 0x7F) << 40;

        uint64_t res_significand = significand;
        int8_t res_exponent = (i >> 48) & 0xFF;
        bool res_negative = ((1ULL << 47) & i) != 0;

        return Decimal(res_significand, res_exponent, res_negative);
    }

    constexpr bool can_inline() const
    {
        return (significand & 0xFF'FF'80'00'00'00'00'00) == 0;
    }

    uint64_t serialize_inlined() const
    {
        uint64_t res = static_cast<uint8_t>(exponent);
        res = res << 48;
        if (negative) {
            res |= (1ULL << 47);
        }
        return res | significand;
    }

    bool operator==(const Decimal& rhs) const
    {
        // border case: comparison between +0 and -0
        if (this->significand == 0) {
            return rhs.significand == 0;
        }
        return significand == rhs.significand && exponent == rhs.exponent &&
               negative == rhs.negative;
    }

    bool operator!=(const Decimal& rhs) const
    {
        return !(rhs == *this);
    }

    bool operator<(const Decimal& rhs) const
    {
        if (significand == 0 && rhs.significand == 0) return false;
        if (negative != rhs.negative) return negative;
        if (significand == 0) return !negative;
        if (rhs.significand == 0) return negative;
        auto left = std::to_string(significand);
        auto right = std::to_string(rhs.significand);
        const auto left_digits = static_cast<int>(left.size()) + exponent;
        const auto right_digits = static_cast<int>(right.size()) + rhs.exponent;
        if (left_digits != right_digits) {
            return negative ? left_digits > right_digits : left_digits < right_digits;
        }
        // Align decimal places without truncating low significant digits.
        const auto width = std::max(left.size(), right.size());
        left.resize(width, '0');
        right.resize(width, '0');
        return negative ? left > right : left < right;
    }

    bool operator>(const Decimal& rhs) const
    {
        return rhs < *this;
    }

    bool operator<=(const Decimal& rhs) const
    {
        return !(rhs < *this);
    }

    bool operator>=(const Decimal& rhs) const
    {
        return !(*this < rhs);
    }

    bool is_zero() const
    {
        return significand == 0;
    }

    Decimal operator-() const
    {
        Decimal dec(*this);
        dec.negative = !dec.negative;
        return dec;
    }

    Decimal abs() const
    {
        Decimal res(*this);
        res.negative = false;
        return res;
    }

    Decimal floor() const
    {
        Decimal res(*this);

        if (exponent >= 0) {
            return res;
        }

        for (int i = 0; i < -exponent; i++) {
            res.significand /= 10;
        }

        int adjustment = res.negative ? 1 : 0;
        res.significand += adjustment;
        res.exponent = 0;

        res.normalize();
        return res;
    }

    Decimal ceil() const
    {
        Decimal res(*this);

        if (exponent >= 0) {
            return res;
        }

        for (int i = 0; i < -exponent; i++) {
            res.significand /= 10;
        }

        int adjustment = res.negative ? 0 : 1;
        res.significand += adjustment;
        res.exponent = 0;

        res.normalize();
        return res;
    }

    Decimal round() const
    {
        Decimal res(*this);

        if (this->exponent >= 0) {
            return res;
        }

        for (int i = 0; i < -this->exponent - 1; i++) {
            res.significand /= 10;
        }

        // first digit after .
        auto first_decimal_digit = res.significand % 10;

        int adjustment = 0;

        if (this->negative) {
            // for negative decimals ROUND(-X.5) = (-X)
            if (first_decimal_digit > 5) {
                adjustment = 1;
            } else if (first_decimal_digit == 5 && this->exponent < -1) {
                // assumes this is normalized
                // ROUND(-1.50001) = (-2)
                adjustment = 1;
            }
        } else {
            if (first_decimal_digit >= 5) {
                adjustment = 1;
            }
        }

        res.significand /= 10;
        res.significand += adjustment;
        res.exponent = 0;

        res.normalize();
        return res;
    }

    friend std::ostream& operator<<(std::ostream& os, const Decimal& dec)
    {
        if (dec.negative)
            os << '-';

        if (dec.exponent >= 0) {
            os << dec.significand;
            for (int i = 0; i < dec.exponent; i++) {
                os << '0';
            }
            os << ".0";
        } else {
            char buf[MAX_DIGIT_PRECISION + 1];

            auto digits_significand = snprintf(
                buf,
                MAX_DIGIT_PRECISION + 1,
                "%llu",
                static_cast<unsigned long long>(dec.significand)
            );

            if (digits_significand <= -dec.exponent) {
                os << "0.";

                auto zeros_after_comma = -dec.exponent - digits_significand;
                for (int i = 0; i < zeros_after_comma; i++) {
                    os << '0';
                }
                for (int i = 0; i < digits_significand; i++) {
                    os << buf[i];
                }
            } else {
                auto digits_before_comma = digits_significand + dec.exponent;

                for (int i = 0; i < digits_before_comma; i++) {
                    os << buf[i];
                }
                os << '.';
                for (int i = digits_before_comma; i < digits_significand; i++) {
                    os << buf[i];
                }
            }
        }

        return os;
    }

    Decimal operator+(const Decimal& other) const
    {
        const int common = std::min(exponent, other.exponent);
        BigInt left = BigInt(significand) * power10(exponent - common);
        BigInt right = BigInt(other.significand) * power10(other.exponent - common);
        if (negative) left = -left;
        if (other.negative) right = -right;
        return from_exact(left + right, common);
    }

    Decimal operator-(const Decimal& rhs) const
    {
        return *this + (-rhs);
    }

    Decimal operator*(const Decimal& rhs) const
    {
        BigInt coefficient = BigInt(significand) * rhs.significand;
        if (negative != rhs.negative) coefficient = -coefficient;
        return from_exact(coefficient, static_cast<int>(exponent) + rhs.exponent);
    }

    Decimal operator/(const Decimal& rhs) const
    {
        if (rhs.significand == 0) throw std::domain_error("Division by zero");
        if (significand == 0) return Decimal();
        BigInt numerator = significand;
        const BigInt denominator = rhs.significand;
        int exp = static_cast<int>(exponent) - rhs.exponent;
        while (numerator < denominator) {
            numerator *= 10;
            --exp;
        }
        BigInt quotient = numerator / denominator;
        BigInt remainder = numerator % denominator;
        while (remainder != 0 && quotient.convert_to<std::string>().size() < MAX_DIGIT_PRECISION) {
            numerator *= 10;
            --exp;
            quotient = numerator / denominator;
            remainder = numerator % denominator;
        }
        // The bounded decimal division policy is 19 significant digits, ties to even.
        const BigInt twice = remainder * 2;
        if (twice > denominator || (twice == denominator && quotient % 2 != 0)) ++quotient;
        if (negative != rhs.negative) quotient = -quotient;
        return from_exact(quotient, exp);
    }

    float to_float() const
    {
        return to_double();
    }

    double to_double() const
    {
        double res = significand;
        res *= std::pow(10, exponent);
        if (negative) {
            res *= -1;
        }
        return res;
    }

    // Converts the Decimal to an int64_t, truncation occurs.
    // If the result does not fit into an int64_t an error is returned.
    int64_t to_int(bool* error) const
    {
        *error = false;
        const uint64_t limit = (uint64_t{1} << 63) - (negative ? 0 : 1);
        uint64_t result = significand;
        if (exponent < 0) {
            for (int i = 0; i < -exponent; ++i) result /= 10;
        } else {
            for (int i = 0; i < exponent; ++i) {
                if (result > limit / 10) {
                    *error = true;
                    return 0;
                }
                result *= 10;
            }
        }
        if (result > limit) {
            *error = true;
            return 0;
        }
        if (negative && result == (uint64_t{1} << 63)) return std::numeric_limits<int64_t>::min();
        return negative ? -static_cast<int64_t>(result) : static_cast<int64_t>(result);
    }

    std::string to_string() const
    {
        std::stringstream ss;
        ss << *this;
        return ss.str();
    }

private:
    static BigInt power10(int exponent) {
        BigInt value = 1;
        for (int i = 0; i < exponent; ++i) value *= 10;
        return value;
    }

    static Decimal from_exact(BigInt coefficient, int exponent) {
        if (coefficient == 0) return Decimal();
        const bool negative = coefficient < 0;
        if (negative) coefficient = -coefficient;
        while (coefficient % 10 == 0) {
            coefficient /= 10;
            ++exponent;
        }
        if (exponent < INT8_MIN || exponent > INT8_MAX ||
            coefficient.convert_to<std::string>().size() > MAX_DIGIT_PRECISION) {
            throw std::out_of_range("Unrepresentable decimal");
        }
        return Decimal(coefficient.convert_to<uint64_t>(), static_cast<int8_t>(exponent), negative);
    }

    void normalize()
    {
        BigInt coefficient = significand;
        if (negative) coefficient = -coefficient;
        *this = from_exact(coefficient, exponent);
    }

};

static_assert(std::is_trivially_copyable<Decimal>::value, "Decimal should be trivially copyable");

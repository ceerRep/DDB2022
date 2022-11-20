#include <seastar/rpc/rpc.hh>

#include <type_traits>
#include <vector>
#include <string>

using namespace seastar;

struct serializer {};

template <typename T, typename Output>
inline void write_arithmetic_type(Output &out, T v) {
  static_assert(std::is_arithmetic<T>::value ||
                    std::is_standard_layout<T>::value,
                "must be arithmetic type or standard layout");
  return out.write(reinterpret_cast<const char *>(&v), sizeof(T));
}

template <typename T, typename Input> inline T read_arithmetic_type(Input &in) {
  static_assert(std::is_arithmetic<T>::value ||
                    std::is_standard_layout<T>::value,
                "must be arithmetic type or standard layout");
  T v;
  in.read(reinterpret_cast<char *>(&v), sizeof(T));
  return v;
}

template <typename Output>
inline void write(serializer, Output &output, int32_t v) {
  return write_arithmetic_type(output, v);
}
template <typename Output>
inline void write(serializer, Output &output, uint32_t v) {
  return write_arithmetic_type(output, v);
}
template <typename Output>
inline void write(serializer, Output &output, int64_t v) {
  return write_arithmetic_type(output, v);
}
template <typename Output>
inline void write(serializer, Output &output, uint64_t v) {
  return write_arithmetic_type(output, v);
}
template <typename Output>
inline void write(serializer, Output &output, double v) {
  return write_arithmetic_type(output, v);
}

template <typename Input>
inline int32_t read(serializer, Input &input, rpc::type<int32_t>) {
  return read_arithmetic_type<int32_t>(input);
}
template <typename Input>
inline uint32_t read(serializer, Input &input, rpc::type<uint32_t>) {
  return read_arithmetic_type<uint32_t>(input);
}
template <typename Input>
inline uint64_t read(serializer, Input &input, rpc::type<uint64_t>) {
  return read_arithmetic_type<uint64_t>(input);
}
template <typename Input>
inline uint64_t read(serializer, Input &input, rpc::type<int64_t>) {
  return read_arithmetic_type<int64_t>(input);
}
template <typename Input>
inline double read(serializer, Input &input, rpc::type<double>) {
  return read_arithmetic_type<double>(input);
}

template <typename Output>
inline void write(serializer, Output &out, const std::string &v) {
  write_arithmetic_type(out, uint32_t(v.size()));
  out.write(v.c_str(), v.size());
}

template <typename Input>
inline std::string read(serializer, Input &in, rpc::type<std::string>) {
  auto size = read_arithmetic_type<uint32_t>(in);
  std::string ret = uninitialized_string(size);
  in.read(ret.data(), size);
  return ret;
}

template <typename Output, typename Container>
inline void write_container(serializer s, Output &output, const Container& container)
{
  write_arithmetic_type(output, uint32_t(container.size()));
  for (auto &&value : container)
  {
    write(s, output, value);
  }
}

template <typename Input, typename Container>
inline Container read_container(serializer s, Input &in, rpc::type<Container>)
{
  Container container;
  auto size = read_arithmetic_type<uint32_t>(in);
  container.resize(size);
  for (auto &&value : container)
  {
    value = read(s, in, rpc::type<std::remove_reference_t<decltype(value)>>());
  }

  return container;
}

template <typename Output, typename ValueType>
inline void write(serializer s, Output &output, const std::vector<ValueType>& container)
{
  write_container(s, output, container);
}

template <typename Input, typename ValueType>
inline std::vector<ValueType> read(serializer s, Input &in, rpc::type<std::vector<ValueType>> type)
{
  return read_container(s, in, type);
}

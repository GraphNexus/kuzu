#pragma

#include <vector>
#include <unordered_set>

namespace kuzu {
namespace common {

template<typename T>
struct DistinctVector {
    std::vector<T> values;

    void add(const T& val) {
        if (set.contains(val)) {
            return ;
        }
        values.push_back(val);
        set.insert(val);
    }

private:
    std::unordered_set<T> set;
};

}
}
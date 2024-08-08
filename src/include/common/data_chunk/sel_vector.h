#pragma once

#include <memory>
#include <optional>

#include "common/constants.h"
#include "common/types/types.h"
#include <span>

namespace kuzu {
namespace common {

class SelectionVector {
public:
    KUZU_API static const sel_t INCREMENTAL_SELECTED_POS[DEFAULT_VECTOR_CAPACITY];

    // Mutable selection vector which allocates a fixed-sized buffer
    explicit SelectionVector(sel_t capacity)
        : selectedSize{0}, capacity{capacity},
          selectedPositionsBuffer{std::make_unique<sel_t[]>(capacity)} {
        setToUnfiltered();
    }

    SelectionVector() : SelectionVector{DEFAULT_VECTOR_CAPACITY} {}

    // Immutable selection vector that is a subset of another selection vector
    SelectionVector(const SelectionVector& other, uint64_t indexInOther = 0,
        std::optional<uint64_t> selectedSize = std::nullopt)
        : selectedSize{selectedSize.value_or(other.selectedSize - indexInOther)},
          capacity{other.capacity - indexInOther},
          selectedPositions{other.selectedPositions + indexInOther} {
        KU_ASSERT(!selectedSize || selectedSize <= other.selectedSize - indexInOther);
    }

    bool isUnfiltered() const { return selectedPositions == INCREMENTAL_SELECTED_POS; }

    void setToUnfiltered() { selectedPositions = (sel_t*)&INCREMENTAL_SELECTED_POS; }
    void setToUnfiltered(sel_t size) {
        KU_ASSERT(size <= capacity);
        selectedPositions = (sel_t*)&INCREMENTAL_SELECTED_POS;
        selectedSize = size;
    }

    // Set to filtered is not very accurate. It sets selectedPositions to a mutable array.
    void setToFiltered() {
        if (!selectedPositionsBuffer) {
            selectedPositionsBuffer = std::make_unique<sel_t[]>(DEFAULT_VECTOR_CAPACITY);
        }
        selectedPositions = selectedPositionsBuffer.get();
    }
    void setToFiltered(sel_t size) {
        KU_ASSERT(size <= capacity && selectedPositionsBuffer);
        selectedPositions = selectedPositionsBuffer.get();
        selectedSize = size;
    }

    std::span<sel_t> getMutableBuffer() {
        KU_ASSERT(selectedPositionsBuffer);
        return std::span<sel_t>(selectedPositionsBuffer.get(), capacity);
    }
    std::span<const sel_t> getSelectedPositions() const {
        return std::span<const sel_t>(selectedPositions, selectedSize);
    }

    sel_t getSelSize() const { return selectedSize; }
    void setSelSize(sel_t size) {
        KU_ASSERT(!selectedPositionsBuffer || size <= capacity);
        selectedSize = size;
    }
    void incrementSelSize(sel_t increment = 1) {
        KU_ASSERT(selectedSize < capacity);
        selectedSize += increment;
    }

    sel_t operator[](sel_t index) const {
        KU_ASSERT(index < capacity);
        return selectedPositions[index];
    }
    sel_t& operator[](sel_t index) {
        KU_ASSERT(index < capacity);
        return selectedPositions[index];
    }

private:
    sel_t selectedSize;
    sel_t capacity;
    std::unique_ptr<sel_t[]> selectedPositionsBuffer;
    sel_t* selectedPositions;
};

} // namespace common
} // namespace kuzu

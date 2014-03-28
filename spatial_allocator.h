/*
	Interface of a memory allocator.
	Contains two different ways of allocation:
		- returning a pointer immendiately on calling 'alloc' (like malloc)
		- returning stub value from 'alloc' and querying pointers after calling 'finish' (bulk allocation)

	Note, that alloc has data type hash as the 4-th parameter. This parameter is important if the framework needs
	to distinguish data types. Perhaps, the framework doesn't need to know about it yet now some implementations use
	this knowledge.

*/
#pragma once

#include <map>

#ifdef __linux__
#include <stdlib.h>
#include <memory.h>
#define override
#endif

static const void* ASK_POINTER_AFTER_FINALIZE = (void*)-1;


template<typename coord_t>
class base_alloc {

protected:
	virtual void finish() {}; // 'finilize' is predefined word
	virtual void *alloc(const coord_t& coord, const char* const data, size_t size, unsigned datatype_hash) = 0;
	virtual void* get_ptr(const coord_t& coord) = 0;
	virtual ~base_alloc() {};
};


// Example. Implementations should be in different files.
template<typename coord_t>
class naive_alloc : public base_alloc<coord_t> {
	
	std::map<coord_t, void*> coord_mapping;

public:
	void *alloc(const coord_t& coord, const char* const data, size_t size, unsigned datatype_hash = 0) override {
		void* mem = malloc(size);
		coord_mapping[coord] = mem;
		memcpy(mem, data, size);
		return const_cast<void*>(ASK_POINTER_AFTER_FINALIZE);
	}

	void finish() override {
	}

	void* get_ptr(const coord_t& coords) {
		return coord_mapping[coords];
	}

	~naive_alloc() override {
		// free all
		// #TODO
	}
};

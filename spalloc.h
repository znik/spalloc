/*
	Implementation of the bulk allocator.
	<Description goes here>

	Maintainers:
		nik.zaborovsky@gmail.com, Mar 2014
*/

#pragma once

#ifdef _WIN32
#include <Windows.h>
#endif

#include <algorithm>
#include <functional>
#include <sstream>
#include <cassert>
#include <map>
#include <string>
#include <vector>
#include <memory.h>

#include "storage.h"
#include "spatial_allocator.h"


#ifdef _WIN32
const TCHAR *mappedFile = TEXT("mapped.file");
#endif


unsigned hashing = 1000000;
#define _PRINT_LAYOUT


template<unsigned N>
struct signature_t {
	unsigned u[N];

	void print() const {
		for (int i = N - 1; i >= 0; --i) {
			printf("%u.", u[i]);
		}
		printf("\n");
	}
};

template<unsigned N>
bool operator<(const signature_t<N>& s1, const signature_t<N>& s2) {
	return 0 < memcmp(&s1, &s2, sizeof(s2));
}


// Every dataitem should be derived from this interface.
template<unsigned N>
struct dataitem_t {
public:
	template <typename T>
	T* get() const {
		if ((unsigned)type_hash != (unsigned)typeid(T).hash_code())
			return 0;
		assert(0 != dynamic_cast<const T *const>(this));
		return const_cast<T*>(static_cast<const T *const>(this));
	}

	template <typename T>
	T* get_direct() const {
		return (T*)this;
	}

	virtual ~dataitem_t() {}; // * makes the structure polymorhpic
	virtual void print() = 0;
	size_t	type_hash; // * will be initialized after first usage!
	size_t	type_size; // *
	signature_t<N> coords;
};


template <unsigned N>
class spalloc {

public:
	typedef unsigned cluster_id;
	typedef cluster_id (*f_clusterize)(dataitem_t<N> *item);

private:
	typedef std::vector<dataitem_t<N> *> cluster_t;
	typedef std::map<signature_t<N>, cluster_t> virtual_buckets_t;

	typedef std::map<cluster_id, cluster_t> clusters_t;
	typedef std::map<size_t, clusters_t> clusters_and_types_t;
	typedef std::vector<clusters_and_types_t> maps_t;


public:

	explicit spalloc() : layout(0), m_datasize(0) {}

	~spalloc() {
		if (0 != layout) {
#ifndef _WIN32
			delete [] layout;
#endif
		}
		layout = 0;
	};

public:
	void* get(const signature_t<N>& coord) {
		return mapping[coord]; 
	}

	void put(const char *const data, size_t size, const signature_t<N>& where,
		unsigned type_hash = 0x12345678) {
		//compile_time_check_not_derived_from_dataitem_t<T>();
		//assert(0 != dynamic_cast<const dataitem_t *>(&what));	
		//if (0 == static_cast<const dataitem_t<N> *>(&what))
		//	return;
		//T *copy = new T(what);
		char *copy = static_cast<char*>(malloc(size));
		dataitem_t<N> *di = reinterpret_cast<dataitem_t<N> *>(copy);
		di->type_hash = type_hash; //(unsigned)typeid(T).hash_code();
		di->type_size = size; //sizeof(T);
		di->coords = where;
		memcpy(copy + sizeof(dataitem_t<N>), data + sizeof(dataitem_t<N>), size - sizeof(dataitem_t<N>));
		// * Form buckets at this point
		// * original coordinates will be stored in the data structures
		signature_t<N> bucket_coord = where;
		for (unsigned n = 0; n < N; ++n) {
			bucket_coord.u[n] /= hashing;
		}

		vbuckets[di->type_hash][bucket_coord].push_back(di);
	};

	void make_layout_3() {
		for (auto it = vbuckets.begin(); vbuckets.end() != it; ++it) {
			printf("\nBUCKETS_NUM: %u\n", (unsigned)it->second.size());
		}

		// TODO: to order items we need to know relations between them
		//
		//struct _comparator {
		//	const std::vector<f_clusterize> &clusterize;
		//
		//	_comparator(const std::vector<f_clusterize> &_clusterize) : clusterize(_clusterize) {};
		//
		//	bool operator() (dataitem_t *a, dataitem_t *b) {
		//		for (int i = N - 1; i >= 0; --i) {
		//			if (clusterize[i](a) != clusterize[i](b))
		//				return clusterize[i](a) > clusterize[i](b);
		//		}
		//		return false;
		//	}
		//} comparator(clusterize);
		//
		//for (typename hash_and_vbuckets_t::iterator it = vbuckets.begin(); vbuckets.end() != it; ++it) {
		//	for (typename virtual_buckets_t::iterator it2 = it->second.begin(); it->second.end() != it2; ++it2) {
		//		std::sort(it2->second.begin(), it2->second.end(), comparator);
		//	}
		//}

#ifdef _PRINT_LAYOUT
			printf("LAYOUT:\n\n");
			for (auto it = vbuckets.begin(); vbuckets.end() != it; ++it) {
				for (auto it2 = it->second.rbegin(); it->second.rend() != it2; ++it2) {
					printf("---- BUCKET: ");
					const signature_t<N> &s = it2->first;
					s.print();
					const cluster_t &c = it2->second;
					for (int i = c.size() - 1; i >= 0; --i) {
						//c[i]->print();
						//printf("\n");
						// #NOTE If this is the known type then print out its fields manually
						// #NOTE c[i]->type_size, c[i]->type_hash
					}
				}
			}
#endif

		size_t datatotal = 0;

		// maps - items ordered by types in every bucket
		// maps[i] - items according to one distribution i
		// maps[i][cluster_id] - all items from bucket i that correspond to cluster_id
		// maps[i][cluster_id][hash] - all items from bucket i that correspond to cluster_id of type hash
		typedef std::vector<item_offset_t> offsets_t;
		std::vector<std::map<cluster_id, std::map<size_t, offsets_t> > > maps(N);

		// types
		// hash(T) - sizeof(T) pairs
		std::map<size_t, size_t> types;

		// data_size is total amount of memory that is occupied with data items
		// (*including service fields of dataitem_t base class)
		size_t data_size = 0;
		for (unsigned n = 0; n < N; ++n) {
			size_t off_of_item = 0;
			for (auto it = vbuckets.rbegin(); vbuckets.rend() != it; ++it) {
				for (auto it2 = it->second.rbegin(); it->second.rend() != it2; ++it2) {
					const cluster_t &c = it2->second;
					const signature_t<N> &s = it2->first;
					for (unsigned i = 0; i < c.size(); ++i) {
						maps[n][c[i]->coords.u[n]][(unsigned)c[i]->type_hash].push_back(off_of_item);
						types[(unsigned)c[i]->type_hash] = c[i]->type_size;
						off_of_item += c[i]->type_size;
					}
				}
			}
			if (data_size) {
				assert(data_size == off_of_item && "Amount of memory occupied by data items is the same");
			}
			data_size = off_of_item;
		}

		// Add the file header
		datatotal += sizeof(layout_file_header_begin) + sizeof(item_type_desc_t) * types.size();
		datatotal += sizeof(layout_file_header_end) + N * sizeof(list_offset_t);

		const size_t c_offset_after_headers_and_first_table = datatotal;

		std::vector<list_offset_t> lvl2_tbl;
		std::vector<list_offset_t> lvl3_tbl;

		// Offsets in each table are beginning of the table - based.
		lvl2_tbl.push_back(0);
		lvl3_tbl.push_back(0);

		size_t off;
		// Second level and 3rd level tables
		for (unsigned i = 0; i < N; ++i) {
			// maps[i].size() - number of clusters for proj i
			// + size
			
			// 2nd level table.
			// Stores pointers to 3-rd level lists.
			off = sizeof(list_t) + maps[i].size() * types.size() * sizeof(item_offset_t);
			lvl2_tbl.push_back(lvl2_tbl.back() + off);

			for (auto it1 = maps[i].begin();
				maps[i].end() != it1; ++it1) {

				for (auto v = types.begin(); v != types.end(); ++v) {
					offsets_t &entry = (it1->second)[v->first];

					// 3rd
					// it2->second.size() - number of items in the cluster for proj i
					// + size

					// 3rd level tables.
					// Offsets to all items of the given type and from the given cluster.

					off = sizeof(list_t) + entry.size() * sizeof(item_offset_t);
					lvl3_tbl.push_back(lvl3_tbl.back() + off);
				}
			}
		}
		assert(lvl2_tbl.size() - 1 == N);

		datatotal += lvl2_tbl.back() + lvl3_tbl.back();
		
		const size_t c_offset_dataitems = datatotal;

		assert(0 == layout);
		if (0 != layout)
			free(layout);

		datatotal += data_size;
		m_datasize = data_size;
		m_dataStarts = c_offset_dataitems;
		size_t c_offset_file_end = datatotal;

		// -> 0x0
		// HEADER
		// queries[0] tbl
		// -> [c_offset_after_headers_and_first_table]
		// clusters&types[0] tbl
		// -> [c_offset_dataitems]
		// data items
		// -> [c_offset_file_end]

#ifdef _WIN32
		HANDLE hMapFile = CreateFile(mappedFile, GENERIC_READ | GENERIC_WRITE,
			FILE_SHARE_READ | FILE_SHARE_WRITE,
			0, TRUNCATE_EXISTING, 0, 0);
		if (INVALID_HANDLE_VALUE == hMapFile)
			hMapFile = CreateFile(mappedFile, GENERIC_READ | GENERIC_WRITE,
			FILE_SHARE_READ | FILE_SHARE_WRITE,
			0, CREATE_ALWAYS, 0, 0);
		HANDLE hMapping = CreateFileMapping(hMapFile, 0, PAGE_READWRITE, 0, datatotal, 0);
		assert(INVALID_HANDLE_VALUE != hMapping && "Can't create the mapping");
		layout = static_cast<char*>(MapViewOfFile(hMapping, FILE_MAP_ALL_ACCESS, 0, 0, datatotal));
		assert(0 != layout && "Can't map the file");
#else
		printf("m_dataSize=%u, layout.size=%u\n", m_datasize, datatotal - m_datasize);
		layout = new char[datatotal];
#endif
		// Fill the file header
		layout_file_header_begin *hdr = reinterpret_cast<layout_file_header_begin*>(layout);
		hdr->fileSignature			= 0x5A4B494E;
		hdr->fileVersion			= THIS_BUILD_VERSION;
		hdr->dataitem_types_count	= types.size();
		unsigned i = 0;
		for (auto it = types.begin(); types.end() != it; ++it, ++i) {
			hdr->item_types[i].thash = it->first;
			hdr->item_types[i].tsize = it->second;
		}

		layout_file_header_end *hdr2 = reinterpret_cast<layout_file_header_end*>(layout +
			sizeof(layout_file_header_begin) + types.size() * sizeof(item_type_desc_t));
		hdr2->distributions_count = N;

		// Fill 1st level table - queries
		for (unsigned i = 0; i < lvl2_tbl.size() - 1; ++i) {
			hdr2->dist_list_offset[i] = c_offset_after_headers_and_first_table + lvl2_tbl[i];
		}

		// Fill 2nd level table - clusters and sub-clusters (type) for each distribution
		unsigned num, index = 0;
		for (unsigned i = 0; i < N; ++i) {
			size_t offset_to_proj_list = c_offset_after_headers_and_first_table + lvl2_tbl[i];
			num = 0;
			list_t *list = reinterpret_cast<list_t*>(layout + offset_to_proj_list);
			list->count = types.size() * maps[i].size();

			for (unsigned j = 0; j < maps[i].size(); j++) {
				for (unsigned k = 0; k < types.size(); ++k) {
					list->items[num] = c_offset_after_headers_and_first_table + lvl2_tbl.back() + lvl3_tbl[index];
					num ++;
					index++;
				}
			}
		}

		// Fill 3rd level table - lists of item offsets
		// Each list corresponds to distribution i, cluster it1 and item type it2.
		const size_t third_lvl_table_offset = c_offset_after_headers_and_first_table + lvl2_tbl.back();
		num = 0;
		for (unsigned i = 0; i < N; ++i) {
			for (auto it1 = maps[i].begin();
				maps[i].end() != it1; ++it1) {

				for (auto it2 = it1->second.begin(); it1->second.end() != it2;
					++it2) {
					list_t* list = reinterpret_cast<list_t*>(layout + third_lvl_table_offset + lvl3_tbl[num]);
					list->count = it2->second.size();
					for (int b = list->count - 1; b >= 0; --b) {
						list->items[b] = it2->second[b] + c_offset_dataitems;
					}
			//		assert(sizeof(item_offset_t) * it2->second.size() + sizeof(list_t) ==
			//			lvl3_tbl[num + 1] - lvl3_tbl[num] && "Offsets in tbl3 are not correct!");
					num++;
				}
			}
		}

#ifdef _PRINT_LAYOUT
		printf("\nHOW DATA IS PLACED ON DISK...\n");
#endif
		// Copy data items
		char *data = layout + c_offset_dataitems;
		size_t off_of_item = 0;
		for (auto it = vbuckets.rbegin(); vbuckets.rend() != it; ++it) {
			for (auto it2 = it->second.rbegin(); it->second.rend() != it2; ++it2) {
				const cluster_t &c = it2->second;
				for (int i = c.size() - 1; i >= 0 ; --i) {
					memcpy(data + off_of_item, c[i], c[i]->type_size);
#ifdef _PRINT_LAYOUT
					//c[i]->print();
					// #NOTE print manually
					printf("->%X \n", data + off_of_item);
#endif
					off_of_item += c[i]->type_size;
				}
			}
		}
		assert(off_of_item == c_offset_file_end - c_offset_dataitems &&
			"Data entries size does not match to pre-calculated size");

		// Cleanup memory
		for (auto it = vbuckets.begin(); vbuckets.end() != it; ++it) {
			for (auto it2 = it->second.begin(); it->second.end() != it2; ++it2) {
				const cluster_t &c = it2->second;
				for (unsigned i = 0; i < c.size(); ++i) {
					free(c[i]);
				}
			}
		}
		vbuckets.clear();

#ifdef _WIN32
		UnmapViewOfFile(layout);
		CloseHandle(hMapping);
		CloseHandle(hMapFile);
		layout = 0;
#endif
	}

	void load_file() {
#ifdef _WIN32
		printf("\nLoading from file...\n");
		HANDLE hMapFile = CreateFile(mappedFile, GENERIC_READ | GENERIC_WRITE,
			FILE_SHARE_READ | FILE_SHARE_WRITE,
			0, OPEN_EXISTING, 0, 0);
		if (INVALID_HANDLE_VALUE == hMapFile) {
			assert(false && "No mapping file found...");
			return;
		}

		size_t fileSize = GetFileSize(hMapFile, 0);
		HANDLE hMapping = CreateFileMapping(hMapFile, 0, PAGE_READWRITE, 0, fileSize, 0);
		assert(layout == 0 && "layout should be zero if you load it");
		layout = static_cast<char*>(MapViewOfFile(hMapping, FILE_MAP_READ | FILE_MAP_WRITE, 0, 0, fileSize));
#endif
		assert(0 != layout);
		layout_file_header_begin* hdr = reinterpret_cast<layout_file_header_begin*>(layout);
		m_kinds.resize(hdr->dataitem_types_count);
		for (int i = 0; i < hdr->dataitem_types_count; ++i) {
			m_types[hdr->item_types[i].thash] = hdr->item_types[i].tsize;
			m_kinds[i] = (unsigned)hdr->item_types[i].thash;
		}

		layout_file_header_end* hdr2 = reinterpret_cast<layout_file_header_end*>(reinterpret_cast<char*>(hdr) +
			sizeof(*hdr) + hdr->dataitem_types_count * sizeof(item_type_desc_t));
		
		m_maps.resize(hdr2->distributions_count);
#ifdef _PRINT_LAYOUT
		printf("\nREADING FROM DISK:\n");
#endif
		for (int i = 0; i < hdr2->distributions_count; ++i) {
			list_offset_t lvl2_list_off = hdr2->dist_list_offset[i];
			list_t *lvl2_list = reinterpret_cast<list_t*>(reinterpret_cast<char*>(layout) + lvl2_list_off);
			auto type_index = m_types.begin();
			for (int j = 0; j < lvl2_list->count; ++j) {

				list_offset_t lvl3_list_off = lvl2_list->items[j];
				list_t *lvl3_list = reinterpret_cast<list_t*>(reinterpret_cast<char*>(layout) + lvl3_list_off);

				for (int k = 0; k < lvl3_list->count; ++k) {

					item_offset_t item_off = lvl3_list->items[k];
					dataitem_t<N> *item = reinterpret_cast<dataitem_t<N>*>(reinterpret_cast<char*>(layout) + item_off);	
					m_maps[i][(unsigned)type_index->first][item->coords.u[i] / hashing].push_back(item);
					mapping[item->coords] = item;
#ifdef _PRINT_LAYOUT
					//item->print();
					// #NOTE print manually
#endif
					// i - query no,
					// j / types.size() - cluster no
					// type_index - type no
					// k - data item no
				}
				type_index++;
				if (type_index == m_types.end())
					type_index = m_types.begin();
			}
		}

		// SORTING OF DATA ITEMS INSIDE A CLUSTER (by addresses)
		struct _comparator {
			bool operator() (dataitem_t<N> *a, dataitem_t<N> *b) {
				return (size_t)a < (size_t)b;
			}
		} comparator;

		for (unsigned i1 = 0; i1 < m_maps.size(); ++i1) {
			for (auto i2 = m_maps[i1].begin(); i2 != m_maps[i1].end(); ++i2) {
				for (auto i3 = i2->second.begin(); i3 != i2->second.end(); ++i3) {
					cluster_t &c = i3->second;
					std::sort(c.begin(), c.end(), comparator);
				}
			}
		}

		// SORTING OF CLUSTERS
		maps_t newmap(m_maps.size());
		for (unsigned i1 = 0; i1 < m_maps.size(); ++i1) {
			for (auto i2 = m_maps[i1].begin(); i2 != m_maps[i1].end(); ++i2) {
				for (auto i3 = i2->second.begin(); i3 != i2->second.end(); ++i3) {
					newmap[i1][i2->first][(size_t)i3->second[0]] = i3->second;
				}
			}
		}

		m_maps = newmap;

#ifdef _PRINT_LAYOUT
		printf("\nQUERIES\n");
		for (unsigned i1 = 0; i1 < m_maps.size(); ++i1) {
			printf("QUERY#%d\n", i1);
			for (auto i2 = m_maps[i1].begin(); i2 != m_maps[i1].end(); ++i2) {
				printf("\tTYPE:%u\n", i2->first);
				for (auto i3 = i2->second.begin(); i3 != i2->second.end(); ++i3) {
					printf("\t\tCLUSTER#%d\n", i3->first);
					const cluster_t &c = i3->second;
					for (unsigned i4 = 0; i4 < c.size(); ++i4) {
						//printf("\t\t\t");
						//c[i4]->print();
						//printf("\n");
						// #NOTE print manually
					}
				}
			}
		}
#endif

		printf("Layout loaded.\n");
	}


	void clearMaps() {
		m_maps.clear();
	}


private: // VIRTUAL BUCKETING
	typedef std::map<unsigned, virtual_buckets_t> hash_and_vbuckets_t;
	hash_and_vbuckets_t vbuckets;

	std::vector<f_clusterize> clusterize;

	// After loading
	maps_t m_maps;
	std::map<signature_t<N>, dataitem_t<N>*> mapping;
	std::map<size_t, size_t> m_types;
	std::vector<size_t> m_kinds;		// hashes in array
public:
	size_t m_datasize;
	size_t m_dataStarts;

private: // AUX::DERIVIATION CHECKING

	// Magic to determine at compile time that a data item was not derived from the base class.
	template<typename T>
	void compile_time_check_not_derived_from_dataitem_t() {
		T derived;
		enum inherited { val = (sizeof(test(derived)) == sizeof(char)) }; 
		static_assert(inherited::val, "DataItem class should be derived from dataitem_t");
	}

	char test(const dataitem_t<N>&);
	char (&test(...))[2];

private: // LAYOUT
	char *layout;
};


template<typename coord_t>
class bulk_alloc : public base_alloc<coord_t> {

	typedef spalloc<1> allocator_t;
	allocator_t l;

public:
	void *alloc(const coord_t& coord, const char* const data, size_t size, unsigned datatype_hash = 0) override {
		assert(0 != datatype_hash && "please fill 4-th parameter with 'typeid(your_type).hash_code()'");
		l.put(data, size, coord, datatype_hash);
		return const_cast<void*>(ASK_POINTER_AFTER_FINALIZE);
	}

	void finish() override {
		l.make_layout_3(); // If the file is already there, this step can be omitted.
		l.load_file();
	}

	void* get_ptr(const coord_t& coord) {
		return l.get(coord);
	}
};


/*
	<Description>

	Maintainers:
		nik.zaborovsky@gmail.com, Feb-Mar 2014
*/
#pragma once


#define THIS_BUILD_VERSION	0x00000001

#ifndef _WIN32
typedef int32_t __int32;
#endif

typedef __int32 item_offset_t;
typedef __int32 list_offset_t;


#ifdef _WIN32
#pragma warning(disable:4200)
#endif

#pragma pack(push, 1)

struct item_type_desc_t {
	__int32 thash;	// typeid(T).hash()
	__int32 tsize;	// typesize = POD struct size
};

struct layout_file_header_begin {
	__int32				fileSignature;			// 'zabs'
	__int32				fileVersion;			// THIS_BUILD_VERSION
	__int32				dataitem_types_count;	// count of types
	item_type_desc_t	item_types[0];			// descriptors of types: 0 <= ... < dataitem_types_count
};

struct layout_file_header_end {
	__int32			distributions_count;		// number of different preprocessed distributions
	list_offset_t	dist_list_offset[0];		// offsets to distributions
};

struct list_t {
	__int32			count;
	item_offset_t	items[0];
};

#pragma pack(pop)

#ifdef _WIN32
#pragma warning(default:4200)
#endif

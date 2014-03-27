/*
	<HEADER>
	
	Maintainers:
		nik.zaborovsky@gmail.com, Mar 2014
*/
#include "spatial_allocator.h"
#include "spalloc.h"


struct type : public dataitem_t<1> {
	unsigned d;

	void print() override {};
};

spalloc<1>::cluster_id cluster(dataitem_t<1> *i) {
	type* obj = i->get_direct<type>();
	return obj->d;
}

bool operator< (const type& t1, const type& t2) {
	return t1.d < t2.d;
}


int main() {

#if 0
	{
		naive_alloc<signature_t<1>> na;
		type t;
		t.d = 123;
		signature_t<1> s = {cluster(&t)};

		void *ptr = na.alloc(s, (const char *const)&t, sizeof(t));
		if (ASK_POINTER_AFTER_FINALIZE != ptr) {
			// do stuff
			return;
		}
		na.finish();
		type *ptr2 = (type *)na.get_ptr(s);
		int stopper = 1;
	}
#endif
	
	bulk_alloc<signature_t<1>> ba; // one-dim bulk allocator
	type t;
	t.d = 0x1234;

	for (unsigned i = 0; i < 100; i++) {
		t.d = i;
		signature_t<1> s = {cluster(&t)};

		void *ptr = ba.alloc(s, (const char* const)&t, sizeof(t), typeid(t).hash_code());
		if (ASK_POINTER_AFTER_FINALIZE != ptr) {
			// do stuff
			assert(false);
			return 0;
		}
	}
	ba.finish();
	signature_t<1> s = {333};

	type *ptr2 = (type *)ba.get_ptr(s);
	int stopper2 = 123;

	return 1;
}
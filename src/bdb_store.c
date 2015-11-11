#include <assert.h>
#include <stdlib.h>
#include <errno.h>

#include <btree/bdb.h>
#include "bdb_common.h"

#define MAX_NAME_LEN 256
#define MAX_DATA_SIZE 1048576

/* store data: u32 refcount, user data */

struct bdb_store {
	DB *primary;
	DB *secondary;
};

static int cb_store_index(
		DB *secondary,
		const DBT *pkey,
		const DBT *pdata,
		DBT *skey)
{
	skey->data = pdata->data + SIZE_U32;
	skey->size = pdata->size - SIZE_U32;
	return 0;
}

/*static int store_get_size(
		DB *db,
		DB_TXN *txn,
		db_recno_t entry)
{}

static int store_get_refcount(
		DB *db,
		DB_TXN *txn,
		db_recno_t entry)
{}

static int store_set_refcount(
		DB *db,
		DB_TXN *txn,
		db_recno_t entry)
{}*/

bdb_store_t *bdb_store_create(
		DB_ENV *env,
		DB_TXN *txn,
		const char *name)
{
	bdb_store_t *self;
	int ret;
	char namebuf[MAX_NAME_LEN + 1 + 8];

	if(strlen(name) > MAX_NAME_LEN) {
		ret = -EINVAL;
		goto error_1;
	}

	self = calloc(1, sizeof(*self));
	if(self == NULL) {
		ret = -EINVAL;
		goto error_1;
	}

	ret = db_create(&self->primary, env, 0);
	if(ret != 0) {
		ret = -EINVAL;
		goto error_2;
	}
	ret = db_create(&self->secondary, env, 0);
	if(ret != 0)
		goto error_3;

	sprintf(namebuf, "%s.st.pri", name);
	ret = self->primary->open(self->primary, txn, namebuf, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0);
	if(ret != 0)
		goto error_4;
	sprintf(namebuf, "%s.st.sec", name);
	ret = self->secondary->open(self->secondary, txn, namebuf, NULL, DB_BTREE, DB_CREATE | DB_TRUNCATE, 0);
	if(ret != 0)
		goto error_4;
	ret = self->primary->associate(self->primary, txn, self->secondary, cb_store_index, DB_IMMUTABLE_KEY);
	if(ret != 0)
		goto error_4;

	return self;

error_4:
	self->secondary->close(self->secondary, 0);
error_3:
	self->primary->close(self->primary, 0);
error_2:
	free(self);
error_1:
	errno = ret;
	return NULL;
}

bdb_store_t *bdb_store_open(
		DB_ENV *env,
		DB_TXN *txn,
		const char *name,
		int flags)
{
	bdb_store_t *self;
	int ret;
	u_int32_t dbflags = 0;
	char namebuf[MAX_NAME_LEN + 1 + 8];

	if(strlen(name) > MAX_NAME_LEN) {
		ret = -EINVAL;
		goto error_1;
	}

	self = calloc(1, sizeof(*self));
	if(self == NULL) {
		ret = -EINVAL;
		goto error_1;
	}

	ret = db_create(&self->primary, env, 0);
	if(ret != 0) {
		ret = -EINVAL;
		goto error_2;
	}
	ret = db_create(&self->secondary, env, 0);
	if(ret != 0)
		goto error_3;

	if((flags & BTREE_RDONLY) != 0)
		dbflags |= DB_RDONLY;

	sprintf(namebuf, "%s.st.pri", name);
	ret = self->primary->open(self->primary, txn, namebuf, NULL, DB_RECNO, dbflags, 0);
	if(ret != 0)
		goto error_4;
	sprintf(namebuf, "%s.st.sec", name);
	ret = self->secondary->open(self->secondary, txn, namebuf, NULL, DB_BTREE, dbflags, 0);
	if(ret != 0)
		goto error_4;
	ret = self->primary->associate(self->primary, txn, self->secondary, cb_store_index, DB_IMMUTABLE_KEY);
	if(ret != 0)
		goto error_4;

	return self;

error_4:
	self->secondary->close(self->secondary, 0);
error_3:
	self->primary->close(self->primary, 0);
error_2:
	free(self);
error_1:
	errno = ret;
	return NULL;
}

void bdb_store_destroy(
		bdb_store_t *self)
{
	self->secondary->close(self->secondary, 0);
	self->primary->close(self->primary, 0);
	free(self);
}

int bdb_store_flush(
		bdb_store_t *self,
		DB_TXN *txn)
{
	int ret;

	ret = self->primary->sync(self->primary, 0);
	if(ret != 0)
		return ret;
	ret = self->secondary->sync(self->secondary, 0);
	if(ret != 0)
		return ret;

	return 0;
}

/* TODO when deleting an entry from a store, remember the deleted recno using a free list and reuse it in _get(); use entry with recno 1 to store free list; simply use DB_DBT_PARTIAL, first 4 bytes for list, since they are available in every case and the index function can. How to delete from secondary db? */

db_recno_t bdb_store_get(
		bdb_store_t *self,
		DB_TXN *txn,
		const void *data,
		int len,
		bool incref)
{
	DBT dbt_pkey;
	DBT dbt_skey;
	DBT dbt_data;
	db_recno_t recno;
	int ret;

	if(len == 0) {
		errno = 0;
		return 0;
	}
	else if(len > MAX_DATA_SIZE) {
		errno = -EFBIG;
		return 0;
	}

	memset(&dbt_pkey, 0, sizeof(dbt_pkey));
	memset(&dbt_skey, 0, sizeof(dbt_skey));
	memset(&dbt_data, 0, sizeof(dbt_data));

	dbt_pkey.data = &recno;
	dbt_pkey.ulen = sizeof(recno);
	dbt_pkey.flags = DB_DBT_USERMEM;
	dbt_skey.data = (void*)data;
	dbt_skey.size = len;
	dbt_skey.flags = DB_DBT_READONLY;

	ret = self->secondary->exists(self->secondary, txn, &dbt_skey, 0);
	if(ret == DB_NOTFOUND) {
		dbt_data.data = malloc(len + SIZE_U32);
		if(dbt_data.data == NULL) {
			errno = -ENOMEM;
			return 0;
		}
		dbt_data.size = len + SIZE_U32;
		if(incref)
			buffer_set_u32(dbt_data.data, 0, 1);
		else
			buffer_set_u32(dbt_data.data, 0, 0);
		buffer_set_data(dbt_data.data, SIZE_U32, data, len);
		ret = self->primary->put(self->primary, txn, &dbt_pkey, &dbt_data, DB_APPEND);
		free(dbt_data.data);
		if(ret != 0) {
			errno = ret;
			return 0;
		}
		return recno;
	}
	else {
		dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;
		ret = self->secondary->pget(self->secondary, txn, &dbt_skey, &dbt_pkey, &dbt_data, 0);
		if(ret != 0) {
			errno = ret;
			return 0;
		}
		if(incref) {
			ret = bdb_store_acquire(self, txn, recno, 1);
			if(ret < 0) {
				errno = ret;
				return 0;
			}
		}
		return recno;
	}
}

db_recno_t bdb_store_try(
		bdb_store_t *self,
		DB_TXN *txn,
		const void *data,
		int len)
{
	DBT dbt_pkey;
	DBT dbt_skey;
	db_recno_t recno;
	int ret;

	if(len == 0) {
		errno = 0;
		return 0;
	}

	memset(&dbt_pkey, 0, sizeof(dbt_pkey));
	memset(&dbt_skey, 0, sizeof(dbt_skey));

	dbt_pkey.data = &recno;
	dbt_pkey.ulen = sizeof(recno);
	dbt_pkey.flags = DB_DBT_USERMEM;
	dbt_skey.data = (void*)data;
	dbt_skey.size = len;
	dbt_skey.flags = DB_DBT_READONLY;

	ret = self->secondary->exists(self->secondary, txn, &dbt_skey, 0);
	if(ret == DB_NOTFOUND) {
		errno = 0;
		return 0;
	}
	else {
		DBT dbt_data;
		memset(&dbt_data, 0, sizeof(dbt_data));
		dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;
		ret = self->secondary->pget(self->secondary, txn, &dbt_skey, &dbt_pkey, &dbt_data, 0);
		if(ret != 0) {
			errno = ret;
			return 0;
		}
		return recno;
	}
}

int bdb_store_size(
		bdb_store_t *self,
		DB_TXN *txn,
		db_recno_t entry)
{
	DBT dbt_key;
	DBT dbt_data;
	int ret;

	memset(&dbt_key, 0, sizeof(dbt_key));
	memset(&dbt_data, 0, sizeof(dbt_data));

	dbt_key.data = &entry;
	dbt_key.size = sizeof(entry);
	dbt_data.flags = DB_DBT_USERMEM;
	ret = self->primary->get(self->primary, txn, &dbt_key, &dbt_data, 0);
	if(ret == DB_BUFFER_SMALL)
		return dbt_data.size - SIZE_U32;
	else {
		errno = ret;
		return 0;
	}
}

int bdb_store_data(
		bdb_store_t *self,
		DB_TXN *txn,
		db_recno_t entry,
		void *data,
		int offset,
		int size)
{
	DBT dbt_key;
	DBT dbt_data;

	memset(&dbt_key, 0, sizeof(dbt_key));
	memset(&dbt_data, 0, sizeof(dbt_data));

	dbt_key.data = &entry;
	dbt_key.size = sizeof(entry);
	dbt_data.data = data;
	dbt_data.ulen = size;
	dbt_data.doff = offset + SIZE_U32;
	dbt_data.dlen = size;
	dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;
	return self->primary->get(self->primary, txn, &dbt_key, &dbt_data, 0);
}

int bdb_store_acquire(
		bdb_store_t *self,
		DB_TXN *txn,
		db_recno_t entry,
		uint32_t amount)
{
	char refcount_buffer[SIZE_U32];
	uint32_t refcount;
	DBT dbt_key;
	DBT dbt_data;
	int ret;

	memset(&dbt_key, 0, sizeof(dbt_key));
	memset(&dbt_data, 0, sizeof(dbt_data));

	dbt_key.data = &entry;
	dbt_key.size = sizeof(entry);
	dbt_data.data = refcount_buffer;
	dbt_data.ulen = SIZE_U32;
	dbt_data.dlen = SIZE_U32;
	dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;
	ret = self->primary->get(self->primary, txn, &dbt_key, &dbt_data, 0);
	if(ret != 0)
		return ret;
	refcount = buffer_get_u32(refcount_buffer, 0);
	refcount += amount;
	if(refcount + amount < refcount)
		return -EINVAL;
	buffer_set_u32(refcount_buffer, 0, refcount);
	ret = self->primary->put(self->primary, txn, &dbt_key, &dbt_data, 0);
	if(ret != 0)
		return ret;
	return 0;
}

int bdb_store_release(
		bdb_store_t *self,
		DB_TXN *txn,
		db_recno_t entry,
		uint32_t amount)
{
	char refcount_buffer[SIZE_U32];
	uint32_t refcount;
	DBT dbt_key;
	DBT dbt_data;
	int ret;

	memset(&dbt_key, 0, sizeof(dbt_key));
	memset(&dbt_data, 0, sizeof(dbt_data));

	dbt_key.data = &entry;
	dbt_key.size = sizeof(entry);
	dbt_data.data = refcount_buffer;
	dbt_data.ulen = SIZE_U32;
	dbt_data.dlen = SIZE_U32;
	dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;
	ret = self->primary->get(self->primary, txn, &dbt_key, &dbt_data, 0);
	if(ret != 0)
		return ret;
	refcount = buffer_get_u32(refcount_buffer, 0);
	if(refcount < amount)
		return -EINVAL;
	refcount -= amount;
	buffer_set_u32(refcount_buffer, 0, refcount);
	if(refcount == 0) {
		ret = self->primary->del(self->primary, txn, &dbt_key, 0);
		if(ret != 0)
			return ret;
	}
	else {
		ret = self->primary->put(self->primary, txn, &dbt_key, &dbt_data, 0);
		if(ret != 0)
			return ret;
	}
	return 0;
}


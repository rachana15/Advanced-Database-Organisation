#include "dberror.h"
#include "btree_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "tables.h"

// Structure that holds actual data of the node
typedef struct NodeData {
	RID rid;
} NodeData;

// Structure to represent B+ Tree Node
typedef struct Node {
	void ** pointers;
	Value ** keys;
	struct Node * parent;
	bool is_leaf;
	int number_of_keys;
	struct Node * next_node; 
} Node;

// Structure to hold extra info of B+ Tree
typedef struct BTreeManager {
	BM_BufferPool bufferPool;
	BM_PageHandle pageHandler;
	int order;
	int number_of_nodes;
	int number_of_enteries;
	Node * root;
	Node * queue;
	DataType datatype;
} Btree_Manager;

// Structure to perform B+ Tree Scan Functions 
typedef struct ScanManager {
	int keyIndex;
	int totalKeys;
	int order;
	Node * node;
} Scan_Manager;


Node * findLeaf(Node * root, Value * key);
NodeData * findRecord(Node * root, Value * key);
void enqueue(Btree_Manager * treeManager, Node * new_node);
Node * dequeue(Btree_Manager * treeManager);
int path_to_root(Node * root, Node * child);
NodeData * makeRecord(RID * rid);
Node * insertIntoLeaf(Btree_Manager * treeManager, Node * leaf, Value * key, NodeData * pointer);
Node * createNewTree(Btree_Manager * treeManager, Value * key, NodeData * pointer);
Node * createNode(Btree_Manager * treeManager);
Node * createLeaf(Btree_Manager * treeManager);
Node * insertIntoParent(Btree_Manager * treeManager, Node * left, Value * key, Node * right);
Node * insertIntoNewRoot(Btree_Manager * treeManager, Node * left, Value * key, Node * right);
Node * insertIntoLeafAfterSplitting(Btree_Manager * treeManager, Node * leaf, Value * key, NodeData * pointer);
Node * insertIntoNode(Btree_Manager * treeManager, Node * parent, int left_index, Value * key, Node * right);
Node * insertIntoNodeAfterSplitting(Btree_Manager * treeManager, Node * parent, int left_index, Value * key, Node * right);
int getLeftIndex(Node * parent, Node * left);
Node * adjustRoot(Node * root);
Node * delete(Btree_Manager * treeManager, Value * key);
Node * mergeNodes(Btree_Manager * treeManager, Node * n, Node * neighbor, int neighbor_index, int k_prime);
Node * redistributeNodes(Node * root, Node * n, Node * neighbor, int neighbor_index, int k_prime_index, int k_prime);
Node * deleteEntry(Btree_Manager * treeManager, Node * n, Value * key, void * pointer);
Node * removeEntryFromNode(Btree_Manager * treeManager, Node * n, Value * key, Node * pointer);
int getNeighborIndex(Node * n);
bool isLess(Value * key1, Value * key2);
bool isGreater(Value * key1, Value * key2);
bool isEqual(Value * key1, Value * key2);

Btree_Manager * treeManager = NULL; // store index manager metadata

// Function to initialize Index Manager
RC initIndexManager(void *mgmtData) {
	initStorageManager();
	return RC_OK;
}

// Function to Shutdown Index Manager
RC shutdownIndexManager() {
	treeManager = NULL;
	return RC_OK;
}

// Function to create B+ Tree with name "idxId"

RC createBtree(char *idxId, DataType keyType, int n) {
	int maxNodes = PAGE_SIZE / sizeof(Node);
	printf("Creating BTree");
	if (n > maxNodes) {
		return RC_ORDER_TOO_HIGH_FOR_PAGE;
	}
	treeManager = (Btree_Manager *) malloc(sizeof(Btree_Manager));
	treeManager->order = n + 2;		// Setting order of B+ Tree
	treeManager->number_of_nodes = 0;		// No nodes initially.
	treeManager->number_of_enteries = 0;	// No entries initially
	treeManager->root = NULL;		// No root node
	treeManager->queue = NULL;		// No node for printing
	treeManager->datatype = keyType;	// Set datatype to "keyType"
	BM_BufferPool * bm = (BM_BufferPool *) malloc(sizeof(BM_BufferPool));
	treeManager->bufferPool = *bm;
	SM_FileHandle fileHandler;
	RC result;
	char data[PAGE_SIZE];
	if ((result = createPageFile(idxId)) != RC_OK)
		return result;
	if ((result = openPageFile(idxId, &fileHandler)) != RC_OK)
		return result;
	if ((result = writeBlock(0, &fileHandler, data)) != RC_OK)
		return result;
	if ((result = closePageFile(&fileHandler)) != RC_OK)
		return result;
	printf(" \n Created Btree \n");
	return (RC_OK);
}

//Function to open existing B+ Tree
RC openBtree(BTreeHandle **tree, char *idxId) {
	*tree = (BTreeHandle *) malloc(sizeof(BTreeHandle)); // Retrieve B+ Tree handle and assign metadata structure
	(*tree)->mgmtData = treeManager;
	printf("\n inside open btree");
	RC result = initBufferPool(&treeManager->bufferPool, idxId, 1000, RS_FIFO, NULL);
	printf("\n buffer pool init"); 
	if (result == RC_OK) {
		printf("\n openBtree SUCCESS");
		return RC_OK;
	}
	return result;
}

//Function to shutdown buffer pool and close B+ Tree . This frees utilized memory space
RC closeBtree(BTreeHandle *tree) {
	Btree_Manager * treeManager = (Btree_Manager*) tree->mgmtData;
	markDirty(&treeManager->bufferPool, &treeManager->pageHandler); // marking the page as dirty 
	shutdownBufferPool(&treeManager->bufferPool);
	free(treeManager); // release memory space
	free(tree);
	return RC_OK;
}


// Function to delete a page associated with and hence the B+ Tree 
RC deleteBtree(char *idxId) {
	RC result;
	if ((result = destroyPageFile(idxId)) != RC_OK)
		return result;
	return RC_OK;
}


//Function to insert new record with specific key and record id.

RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
	Btree_Manager *treeManager = (Btree_Manager *) tree->mgmtData;
	NodeData * pointer;
	Node * leaf;
	int bTreeOrder = treeManager->order;
	if (findRecord(treeManager->root, key) != NULL) { // verify if record with that key already exists
		return RC_IM_KEY_ALREADY_EXISTS;
	}
	pointer = makeRecord(&rid); // creating new record
	if (treeManager->root == NULL) {
		treeManager->root = createNewTree(treeManager, key, pointer);
		return RC_OK;
	}
	leaf = findLeaf(treeManager->root, key); // find a leaf in which key has been inserted
	if (leaf->number_of_keys < bTreeOrder - 1) {
		leaf = insertIntoLeaf(treeManager, leaf, key, pointer);
	} else {
		treeManager->root = insertIntoLeafAfterSplitting(treeManager, leaf, key, pointer);
	}
	return RC_OK;
}

//Function searches B+ Tree with specific key and stores its record id

extern RC findKey(BTreeHandle *tree, Value *key, RID *result) {
	Btree_Manager *treeManager = (Btree_Manager *) tree->mgmtData;
	NodeData * r = findRecord(treeManager->root, key);
	if (r == NULL) { // if key doesnot in tree
		return RC_IM_KEY_NOT_FOUND;
	}
	//  else {
	// 	//printf("NodeData -- key %d, page %d, slot = %d \n", key->v.intV, r->rid.page, r->rid.slot);
	// }
	*result = r->rid;
	return RC_OK;
}

//Function to get number of nodes in tree
RC getNumNodes(BTreeHandle *tree, int *result) {
	Btree_Manager * treeManager = (Btree_Manager *) tree->mgmtData;
	*result = treeManager->number_of_nodes; // output stored in result parameter
	return RC_OK;
}


// Function to get number of entries in the tree
RC getNumEntries(BTreeHandle *tree, int *result) {
	Btree_Manager * treeManager = (Btree_Manager *) tree->mgmtData;
	*result = treeManager->number_of_enteries; // storing the result
	return RC_OK;
}

//Function to get Key datatype in the Tree
RC getKeyType(BTreeHandle *tree, DataType *result) {
	Btree_Manager * treeManager = (Btree_Manager *) tree->mgmtData;
	*result = treeManager->datatype;
	return RC_OK;
}

// Function to delete key and its record 
RC deleteKey(BTreeHandle *tree, Value *key) {
	Btree_Manager *treeManager = (Btree_Manager *) tree->mgmtData;
	treeManager->root = delete(treeManager, key); // deletes entry
	return RC_OK;
}

// Function to initialize scan that goes through each entry in tree
RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
	Btree_Manager *treeManager = (Btree_Manager *) tree->mgmtData;
	Scan_Manager *scanmeta = malloc(sizeof(Scan_Manager)); // retrieve tree scan data
	*handle = malloc(sizeof(BT_ScanHandle)); // allocate memory space
	Node * node = treeManager->root;
	if (treeManager->root == NULL) {
		return RC_NO_RECORDS_TO_SCAN;
	} else {
		while (!node->is_leaf)
			node = node->pointers[0];
		scanmeta->keyIndex = 0;
		scanmeta->totalKeys = node->number_of_keys;
		scanmeta->node = node;
		scanmeta->order = treeManager->order;
		(*handle)->mgmtData = scanmeta;
	}
	return RC_OK;
}

//Function to traverse entries in tree
RC nextEntry(BT_ScanHandle *handle, RID *result) {
	Scan_Manager * scanmeta = (Scan_Manager *) handle->mgmtData;
	int keyIndex = scanmeta->keyIndex; // fetching the info
	int totalKeys = scanmeta->totalKeys;
	int bTreeOrder = scanmeta->order;
	RID rid;
	Node * node = scanmeta->node;
	if (node == NULL) { // if node is empty
		return RC_IM_NO_MORE_ENTRIES;
	}
	if (keyIndex < totalKeys) { // if searched key is present in same leaf
		rid = ((NodeData *) node->pointers[keyIndex])->rid;
		scanmeta->keyIndex++;
	} else { // when leaf node entry not scanned
		if (node->pointers[bTreeOrder - 1] != NULL) {
			node = node->pointers[bTreeOrder - 1];
			scanmeta->keyIndex = 1;
			scanmeta->totalKeys = node->number_of_keys;
			scanmeta->node = node;
			rid = ((NodeData *) node->pointers[0])->rid;
		} else {
			return RC_IM_NO_MORE_ENTRIES;
		}
	}
	*result = rid; // store record id
	return RC_OK;
}

// Function to stop tree scan and deallocate resource
extern RC closeTreeScan(BT_ScanHandle *handle) {
	handle->mgmtData = NULL;
	free(handle);
	return RC_OK;
}

// Function to print B+ Tree
extern char *printTree(BTreeHandle *tree) {
	Btree_Manager *treeManager = (Btree_Manager *) tree->mgmtData;
	Node * n = NULL;
	int i = 0;
	int rank = 0;
	int new_rank = 0;
	if (treeManager->root == NULL) {
		return '\0';
	}
	treeManager->queue = NULL;
	enqueue(treeManager, treeManager->root);
	while (treeManager->queue != NULL) {
		n = dequeue(treeManager);
		if (n->parent != NULL && n == n->parent->pointers[0]) {
			new_rank = path_to_root(treeManager->root, n);
			if (new_rank != rank) {
				rank = new_rank;
				printf("\n");
			}
		}
		for (i = 0; i < n->number_of_keys; i++) { // displaying key depending on datatype
			switch (treeManager->datatype) {
			case DT_INT:
				printf("%d ", (*n->keys[i]).v.intV);
				break;
			case DT_FLOAT:
				printf("%.02f ", (*n->keys[i]).v.floatV);
				break;
			case DT_STRING:
				printf("%s ", (*n->keys[i]).v.stringV);
				break;
			case DT_BOOL:
				printf("%d ", (*n->keys[i]).v.boolV);
				break;
			}
		}
		if (!n->is_leaf)
			for (i = 0; i <= n->number_of_keys; i++)
				enqueue(treeManager, n->pointers[i]);

	}
	return '\0';
}

// Function to make new record that holds key value
NodeData * makeRecord(RID * rid) {
	NodeData * record = (NodeData *) malloc(sizeof(NodeData));
	if (record == NULL) {
		perror("NodeData creation.");
		exit(RC_INSERT_ERROR);
	} else {
		record->rid.page = rid->page;
		record->rid.slot = rid->slot;
	}
	printf("  New NodeData values =%d ..... ", record->rid.page);
	return record;
}

//Function to create new tree as soon as new record is inserted

Node * createNewTree(Btree_Manager * treeManager, Value * key, NodeData * pointer) {

	Node * root = createLeaf(treeManager);
	int bTreeOrder = treeManager->order;
	root->keys[0] = key;
	root->pointers[0] = pointer;
	root->pointers[bTreeOrder - 1] = NULL;
	root->parent = NULL;
	root->number_of_keys++;
	treeManager->number_of_enteries++;
	return root;
}

// Function to add new pointer to record and associated key into leaf
Node * insertIntoLeaf(Btree_Manager * treeManager, Node * leaf, Value * key, NodeData * pointer) {
	int i, insertion_point;
	treeManager->number_of_enteries++;
	insertion_point = 0;
	while (insertion_point < leaf->number_of_keys && isLess(leaf->keys[insertion_point], key))
		insertion_point++;
	for (i = leaf->number_of_keys; i > insertion_point; i--) {
		leaf->keys[i] = leaf->keys[i - 1];
		leaf->pointers[i] = leaf->pointers[i - 1];
	}
	leaf->keys[insertion_point] = key;
	leaf->pointers[insertion_point] = pointer;
	leaf->number_of_keys++;
	return leaf;
}

// Function to add new key and pointer to new record into a leaf
Node * insertIntoLeafAfterSplitting(Btree_Manager * treeManager, Node * leaf, Value * key, NodeData * pointer) {
	Node * new_leaf;
	Value ** temp_keys;
	void ** temp_pointers;
	int insertion_index, split, new_key, i, j;
	new_leaf = createLeaf(treeManager);
	int bTreeOrder = treeManager->order;
	temp_keys = malloc(bTreeOrder * sizeof(Value));
	if (temp_keys == NULL) {
		perror("Temporary keys array.");
		exit(RC_INSERT_ERROR);
	}
	temp_pointers = malloc(bTreeOrder * sizeof(void *));
	if (temp_pointers == NULL) {
		perror("Temporary pointers array.");
		exit(RC_INSERT_ERROR);
	}
	insertion_index = 0;
	while (insertion_index < bTreeOrder - 1 && isLess(leaf->keys[insertion_index], key))
		insertion_index++;

	for (i = 0, j = 0; i < leaf->number_of_keys; i++, j++) {
		if (j == insertion_index)
			j++;
		temp_keys[j] = leaf->keys[i];
		temp_pointers[j] = leaf->pointers[i];
	}
	temp_keys[insertion_index] = key;
	temp_pointers[insertion_index] = pointer;
	leaf->number_of_keys = 0;
	if ((bTreeOrder - 1) % 2 == 0)
		split = (bTreeOrder - 1) / 2;
	else
		split = (bTreeOrder - 1) / 2 + 1;
	for (i = 0; i < split; i++) {
		leaf->pointers[i] = temp_pointers[i];
		leaf->keys[i] = temp_keys[i];
		leaf->number_of_keys++;
	}
	for (i = split, j = 0; i < bTreeOrder; i++, j++) {
		new_leaf->pointers[j] = temp_pointers[i];
		new_leaf->keys[j] = temp_keys[i];
		new_leaf->number_of_keys++;
	}
	free(temp_pointers);
	free(temp_keys);
	new_leaf->pointers[bTreeOrder - 1] = leaf->pointers[bTreeOrder - 1];
	leaf->pointers[bTreeOrder - 1] = new_leaf;
	for (i = leaf->number_of_keys; i < bTreeOrder - 1; i++)
		leaf->pointers[i] = NULL;
	for (i = new_leaf->number_of_keys; i < bTreeOrder - 1; i++)
		new_leaf->pointers[i] = NULL;
	new_leaf->parent = leaf->parent;
	new_key = new_leaf->keys[0];
	treeManager->number_of_enteries++;
	return insertIntoParent(treeManager, leaf, new_key, new_leaf);
}

// Function to insert new key and pointer to a node , making node size to increase
Node * insertIntoNodeAfterSplitting(Btree_Manager * treeManager, Node * old_node, int left_index, Value * key, Node * right) {

	int i, j, split, k_prime;
	Node * new_node, *child;
	Value ** temp_keys;
	Node ** temp_pointers;
	int bTreeOrder = treeManager->order;
	temp_pointers = malloc((bTreeOrder + 1) * sizeof(Node *));
	if (temp_pointers == NULL) {
		perror("Temporary pointers array for splitting nodes.");
		exit(RC_INSERT_ERROR);
	}
	temp_keys = malloc(bTreeOrder * sizeof(Value *));
	if (temp_keys == NULL) {
		perror("Temporary keys array for splitting nodes.");
		exit(RC_INSERT_ERROR);
	}
	for (i = 0, j = 0; i < old_node->number_of_keys + 1; i++, j++) {
		if (j == left_index + 1)
			j++;
		temp_pointers[j] = old_node->pointers[i];
	}
	for (i = 0, j = 0; i < old_node->number_of_keys; i++, j++) {
		if (j == left_index)
			j++;
		temp_keys[j] = old_node->keys[i];
	}
	temp_pointers[left_index + 1] = right;
	temp_keys[left_index] = key;
	if ((bTreeOrder - 1) % 2 == 0) // to copy half of keys into old and rest to new
		split = (bTreeOrder - 1) / 2;
	else
		split = (bTreeOrder - 1) / 2 + 1;
	new_node = createNode(treeManager);
	old_node->number_of_keys = 0;
	for (i = 0; i < split - 1; i++) {
		old_node->pointers[i] = temp_pointers[i];
		old_node->keys[i] = temp_keys[i];
		old_node->number_of_keys++;
	}
	old_node->pointers[i] = temp_pointers[i];
	k_prime = temp_keys[split - 1];
	for (++i, j = 0; i < bTreeOrder; i++, j++) {
		new_node->pointers[j] = temp_pointers[i];
		new_node->keys[j] = temp_keys[i];
		new_node->number_of_keys++;
	}
	new_node->pointers[j] = temp_pointers[i];
	free(temp_pointers);
	free(temp_keys);
	new_node->parent = old_node->parent;
	for (i = 0; i <= new_node->number_of_keys; i++) {
		child = new_node->pointers[i];
		child->parent = new_node;
	}
	treeManager->number_of_enteries++; // adding new key into parent node after splitting 
	return insertIntoParent(treeManager, old_node, k_prime, new_node);
}

//Function to insert leaf or internal node into tree
Node * insertIntoParent(Btree_Manager * treeManager, Node * left, Value * key, Node * right) {
	int left_index;
	Node * parent = left->parent;
	int bTreeOrder = treeManager->order;
	if (parent == NULL) // Checking if it is the new root.
		return insertIntoNewRoot(treeManager, left, key, right);
	left_index = getLeftIndex(parent, left); // finding parents pointer to left node
	if (parent->number_of_keys < bTreeOrder - 1) {
		return insertIntoNode(treeManager, parent, left_index, key, right);
	}
	return insertIntoNodeAfterSplitting(treeManager, parent, left_index, key, right); // splitting node
}

// Function to find index of parents pointer to left key inserted
int getLeftIndex(Node * parent, Node * left) {
	int left_index = 0;
	while (left_index <= parent->number_of_keys && parent->pointers[left_index] != left)
		left_index++;
	return left_index;
}

//Function to insert new key and pointer to a node

Node * insertIntoNode(Btree_Manager * treeManager, Node * parent, int left_index, Value * key, Node * right) {
	int i;
	for (i = parent->number_of_keys; i > left_index; i--) {
		parent->pointers[i + 1] = parent->pointers[i];
		parent->keys[i] = parent->keys[i - 1];
	}
	parent->pointers[left_index + 1] = right;
	parent->keys[left_index] = key;
	parent->number_of_keys++;
	return treeManager->root;
}

// Function to create new root for two subtrees and insert appropriate key into new root
Node * insertIntoNewRoot(Btree_Manager * treeManager, Node * left, Value * key, Node * right) {
	Node * root = createNode(treeManager);
	root->keys[0] = key;
	root->pointers[0] = left;
	root->pointers[1] = right;
	root->number_of_keys++;
	root->parent = NULL;
	left->parent = root;
	right->parent = root;
	return root;
}

// Function to create new general node
Node * createNode(Btree_Manager * treeManager) {
	treeManager->number_of_nodes++;
	int bTreeOrder = treeManager->order;
	Node * new_node = malloc(sizeof(Node));
	if (new_node == NULL) {
		perror("Node creation.");
		exit(RC_INSERT_ERROR);
	}
	new_node->keys = malloc((bTreeOrder - 1) * sizeof(Value *));
	if (new_node->keys == NULL) {
		perror("New node keys array.");
		exit(RC_INSERT_ERROR);
	}
	new_node->pointers = malloc(bTreeOrder * sizeof(void *));
	if (new_node->pointers == NULL) {
		perror("New node pointers array.");
		exit(RC_INSERT_ERROR);
	}
	new_node->is_leaf = false;
	new_node->number_of_keys = 0;
	new_node->parent = NULL;
	new_node->next_node = NULL;
	return new_node;
}


//Function to create new leaf by creating new nde
Node * createLeaf(Btree_Manager * treeManager) {
	Node * leaf = createNode(treeManager);
	leaf->is_leaf = true;
	return leaf;
}

// Function to find the key from root
Node * findLeaf(Node * root, Value * key) {
	int i = 0;
	Node * c = root;
	if (c == NULL) {
		printf("tree is empty\n");
		return c;
	}
	while (!c->is_leaf) {
		i = 0;
		while (i < c->number_of_keys) {
			if (isGreater(key, c->keys[i]) || isEqual(key, c->keys[i])) {
				i++;
			} else
				break;
		}
		c = (Node *) c->pointers[i];
	}
	return c; // return leaf containing given key 
}

//Function to find and return node data for the given key
NodeData * findRecord(Node * root, Value *key) {
	int i = 0;
	Node * c = findLeaf(root, key);
	if (c == NULL)
		return NULL;
	for (i = 0; i < c->number_of_keys; i++) {
		if (isEqual(c->keys[i], key))
			break;
	}
	if (i == c->number_of_keys)
		return NULL;
	else
		return (NodeData *) c->pointers[i];
}

//Function to return index of nodes left nearest neighbour
int getNeighborIndex(Node * n) {
	int i;
	for (i = 0; i <= n->parent->number_of_keys; i++)
		if (n->parent->pointers[i] == n)
			return i - 1;
	printf("Node:  %#lx\n", (unsigned long) n);
	exit(RC_ERROR);
}

// Function to remove record having specific key from given node
Node * removeEntryFromNode(Btree_Manager * treeManager, Node * n, Value * key, Node * pointer) {
	int i, num_pointers;
	int bTreeOrder = treeManager->order;
	i = 0;
	while (!isEqual(n->keys[i], key))
		i++;
	for (++i; i < n->number_of_keys; i++)
		n->keys[i - 1] = n->keys[i];
	num_pointers = n->is_leaf ? n->number_of_keys : n->number_of_keys + 1; // calculate no of pointers
	i = 0;
	while (n->pointers[i] != pointer)
		i++;
	for (++i; i < num_pointers; i++)
		n->pointers[i - 1] = n->pointers[i];
	n->number_of_keys--; // decrement number of keys
	treeManager->number_of_enteries--;
	if (n->is_leaf) // leaf's last pointer points to next leaf
		for (i = n->number_of_keys; i < bTreeOrder - 1; i++)
			n->pointers[i] = NULL;
	else
		for (i = n->number_of_keys + 1; i < bTreeOrder; i++)
			n->pointers[i] = NULL;

	return n;
}

//Function to adjust root after record deletion
Node * adjustRoot(Node * root) {
	Node * new_root;
	if (root->number_of_keys > 0)
		return root;
	if (!root->is_leaf) { // if root is non empty
		new_root = root->pointers[0];
		new_root->parent = NULL;
	} else {
		new_root = NULL; // if root is not empty
	}
	free(root->keys); // free up space
	free(root->pointers);
	free(root);
	return new_root;
}

// Function to merge node after deleting neighbour

Node * mergeNodes(Btree_Manager * treeManager, Node * n, Node * neighbor, int neighbor_index, int k_prime) {
	int i, j, neighbor_insertion_index, n_end;
	Node * tmp;
	int bTreeOrder = treeManager->order;
	if (neighbor_index == -1) { // swapping neighbour nodes
		tmp = n;
		n = neighbor;
		neighbor = tmp;
	}
	neighbor_insertion_index = neighbor->number_of_keys;
	if (!n->is_leaf) { // append k_prime and following pointer , if non leaf node
		neighbor->keys[neighbor_insertion_index] = k_prime;
		neighbor->number_of_keys++;
		n_end = n->number_of_keys;
		for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
			neighbor->keys[i] = n->keys[j];
			neighbor->pointers[i] = n->pointers[j];
			neighbor->number_of_keys++;
			n->number_of_keys--;
		}
		neighbor->pointers[i] = n->pointers[j];
		for (i = 0; i < neighbor->number_of_keys + 1; i++) { // pointing all children to same parent
			tmp = (Node *) neighbor->pointers[i];
			tmp->parent = neighbor;
		}
	} else {
		for (i = neighbor_insertion_index, j = 0; j < n->number_of_keys; i++, j++) { // append keys and pointers of n to neighbor
			neighbor->keys[i] = n->keys[j];
			neighbor->pointers[i] = n->pointers[j];
			neighbor->number_of_keys++;
		}
		neighbor->pointers[bTreeOrder - 1] = n->pointers[bTreeOrder - 1];
	}
	treeManager->root = deleteEntry(treeManager, n->parent, k_prime, n);
	free(n->keys); // free up space
	free(n->pointers);
	free(n);
	return treeManager->root;
}

// Function to delete entry from tree
Node * deleteEntry(Btree_Manager * treeManager, Node * n, Value * key, void * pointer) {
	int min_keys;
	Node * neighbor;
	int neighbor_index;
	int k_prime_index, k_prime;
	int capacity;
	int bTreeOrder = treeManager->order;
	n = removeEntryFromNode(treeManager, n, key, pointer); // remove key and pointer from node
	if (n == treeManager->root) // when n is root
		return adjustRoot(treeManager->root);
	if (n->is_leaf) { // find min allowable size of node
		if ((bTreeOrder - 1) % 2 == 0)
			min_keys = (bTreeOrder - 1) / 2;
		else
			min_keys = (bTreeOrder - 1) / 2 + 1;
	} else {
		if ((bTreeOrder) % 2 == 0)
			min_keys = (bTreeOrder) / 2;
		else
			min_keys = (bTreeOrder) / 2 + 1;
		min_keys--;
	}
	if (n->number_of_keys >= min_keys) // merging when node falls below min
		return treeManager->root;
	neighbor_index = getNeighborIndex(n);
	k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
	k_prime = n->parent->keys[k_prime_index];
	neighbor =
			(neighbor_index == -1) ? n->parent->pointers[1] : n->parent->pointers[neighbor_index];

	capacity = n->is_leaf ? bTreeOrder : bTreeOrder - 1;

	if (neighbor->number_of_keys + n->number_of_keys < capacity)
		return mergeNodes(treeManager, n, neighbor, neighbor_index, k_prime);
	else
		return redistributeNodes(treeManager->root, n, neighbor, neighbor_index, k_prime_index, k_prime); // redistributing 
}

// Function to delete record with specific key 
Node * delete(Btree_Manager * treeManager, Value * key) {
	Node * record = findRecord(treeManager->root, key);
	NodeData * key_leaf = findLeaf(treeManager->root, key);
	if (record != NULL && key_leaf != NULL) {
		treeManager->root = deleteEntry(treeManager, key_leaf, key, record);
		free(record);
	}
	return treeManager->root;
}

// Function to redistribute entries between nodes
Node * redistributeNodes(Node * root, Node * n, Node * neighbor, int neighbor_index, int k_prime_index, int k_prime) {
	int i;
	Node * tmp;

	if (neighbor_index != -1) {
		if (!n->is_leaf)
			n->pointers[n->number_of_keys + 1] = n->pointers[n->number_of_keys];
		for (i = n->number_of_keys; i > 0; i--) {
			n->keys[i] = n->keys[i - 1];
			n->pointers[i] = n->pointers[i - 1];
		}
		if (!n->is_leaf) {
			n->pointers[0] = neighbor->pointers[neighbor->number_of_keys];
			tmp = (Node *) n->pointers[0];
			tmp->parent = n;
			neighbor->pointers[neighbor->number_of_keys] = NULL;
			n->keys[0] = k_prime;
			n->parent->keys[k_prime_index] = neighbor->keys[neighbor->number_of_keys - 1];
		} else {
			n->pointers[0] = neighbor->pointers[neighbor->number_of_keys - 1];
			neighbor->pointers[neighbor->number_of_keys - 1] = NULL;
			n->keys[0] = neighbor->keys[neighbor->number_of_keys - 1];
			n->parent->keys[k_prime_index] = n->keys[0];
		}
	} else { // when n is leftmost child 
		if (n->is_leaf) {
			n->keys[n->number_of_keys] = neighbor->keys[0];
			n->pointers[n->number_of_keys] = neighbor->pointers[0];
			n->parent->keys[k_prime_index] = neighbor->keys[1];
		} else {
			n->keys[n->number_of_keys] = k_prime;
			n->pointers[n->number_of_keys + 1] = neighbor->pointers[0];
			tmp = (Node *) n->pointers[n->number_of_keys + 1];
			tmp->parent = n;
			n->parent->keys[k_prime_index] = neighbor->keys[0];
		}
		for (i = 0; i < neighbor->number_of_keys - 1; i++) {
			neighbor->keys[i] = neighbor->keys[i + 1];
			neighbor->pointers[i] = neighbor->pointers[i + 1];
		}
		if (!n->is_leaf)
			neighbor->pointers[i] = neighbor->pointers[i + 1];
	}
	n->number_of_keys++; // when neighbor has few key and pointer
	neighbor->number_of_keys--;

	return root;
}


//Function to print B+ tree
void enqueue(Btree_Manager * treeManager, Node * new_node) {
	Node * c;
	if (treeManager->queue == NULL) {
		treeManager->queue = new_node;
		treeManager->queue->next_node = NULL;
	} else {
		c = treeManager->queue;
		while (c->next_node != NULL) {
			c = c->next_node;
		}
		c->next_node = new_node;
		new_node->next_node = NULL;
	}
}

//Function to print B+ Tree
Node * dequeue(Btree_Manager * treeManager) {
	Node * n = treeManager->queue;
	treeManager->queue = treeManager->queue->next_node;
	n->next_node = NULL;
	return n;
}

//Function that gives edge length of path from any node to root
int path_to_root(Node * root, Node * child) {
	int length = 0;
	Node * c = child;
	while (c != root) {
		c = c->parent;
		length++;
	}
	return length;
}

//Function to compares two keys . Return TRUE if first key is less than second key
bool isLess(Value * key1, Value * key2) {
	switch (key1->dt) {
	case DT_INT:
		if (key1->v.intV < key2->v.intV) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_FLOAT:
		if (key1->v.floatV < key2->v.floatV) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_STRING:
		if (strcmp(key1->v.stringV, key2->v.stringV) == -1) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_BOOL:
		return FALSE;
		break;
	}
}

//Function to compares two keys . Return TRUE if first key is greater than second key
bool isGreater(Value * key1, Value * key2) {
	switch (key1->dt) {
	case DT_INT:
		if (key1->v.intV > key2->v.intV) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_FLOAT:
		if (key1->v.floatV > key2->v.floatV) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_STRING:
		if (strcmp(key1->v.stringV, key2->v.stringV) == 1) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_BOOL:
		return FALSE;
		break;
	}
}

//Function to compares two keys . Return TRUE if first key is equal to second key
bool isEqual(Value * key1, Value * key2) {
	switch (key1->dt) {
	case DT_INT:
		if (key1->v.intV == key2->v.intV) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_FLOAT:
		if (key1->v.floatV == key2->v.floatV) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_STRING:
		if (strcmp(key1->v.stringV, key2->v.stringV) == 0) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_BOOL:
		if (key1->v.boolV == key2->v.boolV) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	}
}

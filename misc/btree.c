/* 

	This code is used to test teh binary tree functionality that is part of Glib, the Gnome Library.

	The gnome library is commonly supplied on most linux platforms, and is integral to gnome and other standard apps.

	This simple app will create a binary tree, and access data from it.  By testing its functionality, we can be sure 
	that it fits in with our goals with the overall project.

*/


#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <glib.h>


unsigned int generate_hash_str(const char *str, const int length)
{
	register int i;
	register int hash = 2166136261l;

	for (i=0; i<length; i++)  {
		hash ^= (int)str[i];
		hash *= 16777619;
	}

	return(hash);
}

unsigned int generate_hash_int(const int key)
{
	register int i;
	register int hash = 2166136261l;
	union {
		int nkey;
		char str[sizeof(int)];
	} match;
	
	assert(sizeof(key) == 4);
	assert(sizeof(match) == sizeof(key));
	
	match.nkey = ntohl(key);

	for (i=0; i<sizeof(key); i++)  {
		hash ^= (int)match.str[i];
		hash *= 16777619;
	}

	return(hash);
}


gint key_compare_fn(gconstpointer a, gconstpointer b)
{
	const register unsigned int *aa, *bb;
	
	aa = a;
	bb = b;
	
// 	printf("....................comparing\n");
	
	return(*aa - *bb);
}

gboolean traverse_fn(gpointer key, gpointer value, gpointer data)
{
	unsigned int *aa = key;
	static counter=0;
	
	counter++;
	
 	printf("%u='%s'\n", *aa, value); 

	if (counter > 10) 
		return(TRUE);
	else
		return(FALSE);
}


#define LIMIT 1000000

int main(int argc, char **argv)
{
	GTree *tree;
	char *data[5] = {"the fat cat sat on the mat", "the wind this way", "thrice a merry tree", "fire strokes the best of friends", "corner gibblets"};
	unsigned int keys[LIMIT];
	int i;
	
	printf("glib binary tree test.\n\n");

	tree = g_tree_new(key_compare_fn);
	assert(tree);
	
	memset(keys, 0, LIMIT * sizeof(unsigned int));
	
	for (i=0; i<LIMIT; i++) {
	
		if (keys[i] != 0) { printf("collision: %d, key=%u\n", i, keys[i]); }
		keys[i] = generate_hash_int(i);

		g_tree_insert(tree, &keys[i], data[i % 5]);
	}

	printf("nodes in the tree = %d\n", g_tree_nnodes(tree));
	
 	printf("3='%s'\n", g_tree_lookup(tree, &keys[2]));
	
 	printf("\nAll items:\n");
	
 	g_tree_foreach(tree, traverse_fn, NULL);
	
	g_tree_destroy(tree);
	tree = NULL;

	return 0;
}


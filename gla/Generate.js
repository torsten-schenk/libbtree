var exec = module("lang.exec.stack");
var graphml = module("util.graphml");
var maxTrees = 0;
var maxTests = 0;
var lines;
var addtests;

function pn(line) {
	if(line === undefined)
		lines.push("");
	else
		lines.push(line);
}

function addtest(test) {
	addtests.push(test);
}

function load(testname) {
	var byidNode = {};
	var byidTest = {};
	var byidNonLeaf = {};
	var order = null;
	var regexValues = /^(\s*[|]\s*([-]|[+-]?[0-9]*))+(?:\s*[#]\s*([-]|[+-]?[0-9]*))?\s*[|]\s*$/;
	var regexValue = /^\s*([|#])\s*([-]|[+-]?[0-9]*)\s*[|#]/;
	var regexEdge = /^\s*(?:([a-zA-Z_][a-zA-Z0-9_]*)\s*[:]*\s*)?([+-]?[0-9]+|[#])\s*$/;
	var trees = [];
	var tests = [];
	var functions = [];

	var nodeFn = {
		parsed: function(node, graph) {
			switch(node.type) {
				case "shape:roundrectangle":
					var match;
					var label = node.nameLabel;
					var values = [];
					var links = [];
					var fill = 0;
					var overflow = null;

					byidNode[node.id] = node;

					match = regexValues.exec(label);
					if(!match)
						throw Error("Invalid values: '" + label + "'");
//					print("VALUES: " + node.nameLabel);
					while(true) {
						match = regexValue.exec(label);
						if(!match)
							break;
						else if(match[1] === "#") {
//							print("  -> push overflow: " + match[1] + " , " + match[2]);
							overflow = {
								'value': parseInt(match[2]),
								'link': {
									'child': null,
									'count': null,
									'offset': null
								}
							};
						}
						else if(match[1] === "|") {
							if(match[2] === "-" || match[2] === "")
								values.push(null);
							else
								values.push(parseInt(match[2]));
//							print("  -> push: " + match[1] + " , " + match[2]);
							links.push({
								'child': null,
								'count': null,
								'offset': null
							});
						}
						label = label.substring(match[0].length - 1); // -1: don't delete last [|#] (see regexp)
					}
					links.push({
						'child': null,
						'count': null,
						'offset': null
					});
					if(order === null)
						order = values.length + 1;
					else if(order !== values.length + 1)
						throw Error("Invalid order in values '" + node.nameLabel + "'");
					for(var i = 0; i < order - 1; i++)
						if(values[i] === null)
							break;
						else
							fill++;
					for(var i = fill; i < order - 1; i++)
						if(values[i] !== null)
							throw Error("Invalid node elements: gaps not allowed in '" + node.nameLabel + "'");
					if(fill < order - 1 && overflow != null)
						throw Error("Invalid node elements: gaps before overflow element not allowed in '" + node.nameLabel + "'");
					var n = {
						'id': node.id,
						'isleaf': true,
						'fill': fill,
						'name': null,
						'index': null,
						'values': values,
						'overflow': overflow,
						'parent': null,
						'childindex': 0,
						'links': links
					};
					byidNode[node.id] = n;
					break;
				case "shape:triangle":
					byidNonLeaf[node.id] = true;
					break;
				case "er:entity":
					var code = node.nameLabel.match(/[^\r\n]+/g);
					var test = {
						'name': null,
						'origin': null,
						'expected': null,
						'code': code
					};
					tests.push(test);
					byidTest[node.id] = test;
					break;
				case "er:entity_with_attributes":
					var proto = node.nameLabel;
					var body = node.attributeLabel.replace(/  /g, "\t").match(/[^\r\n]+/g);
					functions.push({
						'proto': proto,
						'body': body
					});
					break;
				default:
					throw Error("Unexpected node type: '" + node.type + "'");
			}
		}
	}

	var edgeFn = {
		parsed: function(edge) {
			switch(edge.targetType) {
				case "white_delta":
					var tree;
					var test;
					if(byidNode[edge.sourceId] && byidTest[edge.targetId]) {
						test = byidTest[edge.targetId];
						test['origin'] = edge.sourceId;
						if(edge.nameLabel !== undefined && edge.nameLabel !== "")
							test['name'] = edge.nameLabel;
					}
					else if(byidTest[edge.sourceId] && byidNode[edge.targetId]) {
						test = byidTest[edge.sourceId];
						test['expected'] = edge.targetId;
					}
					else
						throw Error("Invalid test flow");
					break;
				case "standard":
					var index;
					var p;
					var c;
					p = byidNode[edge.sourceId];
					p['isleaf'] = false;
					if(edge.targetId in byidNode) {
						var match = regexEdge.exec(edge.nameLabel);
						var link;
						if(!match)
							throw Error("Invalid edge index: '" + edge.nameLabel + "'");
						if(match[2] === "#") {
							if(p['overflow'] === null)
								throw Error("Overflow edge not allowed on node");
							index = null;
							link = p['overflow']['link'];
						}
						else {
							index = parseInt(match[2]);
							if(index >= order)
								throw Error("Invalid edge index: " + index);
							link = p['links'][index];
						}
						c = byidNode[edge.targetId];
						if(c['parent'] !== null)
							throw Error("Multiple parents for node " + edge.targetId + " not allowed");
						c['parent'] = edge.sourceId;
						c['childindex'] = index;
						if(link.child !== null)
							throw Error("Multiple children at index " + index + " for node " + edge.sourceId + " not allowed");
						link.child = edge.targetId;
						if(match[1] !== undefined)
							c['name'] = match[1];
					}
				break;
				default:
					throw Error("Invalid edge encountered");
			}
		}
	}

	graphml.load("[graphml]local.test." + testname, nodeFn, edgeFn);

	function treeForRoot(root) {
		for(var i in trees)
			if(trees[i]['root'] === root)
				return trees[i];
		return null;
	}

	function toTree(tree, node) {
		node['index'] = tree['nodes'].length;
		tree['nodes'].push(node);
		if(node['name'] !== null) {
			if(tree['named'][node['name']] !== undefined)
				throw Error("Node name '" + node['name'] + "' encountered multiple times in same tree");
			tree['named'][node['name']] = node;
		}
		for(var i = 0; i < order; i++)
			if(node['links'][i].child !== null)
				toTree(tree, byidNode[node['links'][i].child]);
		if(node['overflow'] !== null) {
			if(tree['overflow'] !== null)
				throw Error("Only one overflow node allowed per tree.");
			tree['overflow'] = node;
			if(node['overflow']['link'].child !== null)
				toTree(tree, byidNode[node['overflow']['link'].child]);
		}
	}

	var emptyIndex = 0;
	function createEmpty(p, childindex) {
		var id = '__empty_' + emptyIndex;
		var values = [];
		var links = [];
		for(var i = 0; i < order; i++)
			values.push(null);
		for(var i = 0; i <= order; i++)
			links.push({
				'child': null,
				'count': null
			});
		emptyIndex++;
		byidNode[id] = {
			'id': id,
			'isleaf': true,
			'fill': 0,
			'name': null,
			'index': null,
			'values': values,
			'overflow': null,
			'parent': p,
			'childindex': childindex,
			'links': links
		};
		return id;
	}

	for(var i in byidNode) {
		var n = byidNode[i];
		for(var k = n['fill'] + 1; k < order; k++)
			if(n['links'][k].child !== null)
				throw Error("Invalid links: no more than " + (n['fill'] + 1) + " expected.");
		if(!n['isleaf']) {
			for(var k = 0; k <= n['fill']; k++)
				if(n['links'][k].child === null) {
					n['links'][k].child = createEmpty(i, k);
					n['links'][k].count = null;
				}
			if(n['overflow'] !== null && n['overflow']['link'].child === null) {
				n['overflow']['link'].child = createEmpty(i, null);
				n['overflow']['link'].count = null;
			}
		}
	}

	for(var i in byidNode) {
		var n = byidNode[i];
		if(n['parent'] === null) {
			var tree = {
				'index': trees.length,
				'root': n,
				'nodes': [],
				'overflow': null,
				'named': {}
			};
			toTree(tree, n);
			trees.push(tree);
		}
	}

//	dump(trees);

	function count(n) {
		var c = 0;

		for(var i = 0; i < order; i++) {
			var l = n['links'][i];
			if(l.child !== null && l.count === null) {
				l.offset = c;
				l.count = count(byidNode[l.child]);
			}
			if(l.count !== null)
				c += l.count;
			if(i < order - 1 && n['values'][i] !== null)
				c++;
		}
		if(n['overflow'] !== null) {
			var l = n['overflow']['link'];
			if(n['overflow']['value'] !== null)
				c++;
			if(l.child !== null && l.count === null) {
				l.offset = c;
				l.count = count(byidNode[l.child]);
			}
			if(l.count !== null)
				c += l.count;
		}
		return c;
	}

	for(var i = 0; i < trees.length; i++) {
		var tree = trees[i];
		var root = tree['root'];

		count(root);
	}

	for(var t in trees) {
		var tree = trees[t];
		pn('struct names' + t + '_' + testname + ' {');
		for(var n in tree['named'])
			pn('\tbtree_node_t *' + n + ';');
		pn('};');
		pn('static btree_t *mktree' + t + '_' + testname + '(');
		pn('\t\tstruct names' + t + '_' + testname + ' *names)');
		pn('{');
		pn('\tint i;');
		pn('\tint k;');
		pn('\tbtree_t *tree;');
		pn('\tbtree_node_t *nodes[' + tree['nodes'].length + '];');
		pn();
		pn('\ttree = btree_new(' + order + ', sizeof(int), test_cmp_int, NULL, NULL, BTREE_OPT_MULTI_KEY);');
		pn('\tif(tree == NULL)');
		pn('\t\treturn NULL;');
		for(var i = 0; i < tree['nodes'].length; i++)
			pn('\tnodes[' + i + '] = alloc_node(tree);');
		pn('\tfor(i = 0; i < ' + tree['nodes'].length + '; i++)');
		pn('\t\tfor(k = 0; k < ' + order + '; k++)');
		pn('\t\t\tnodes[i]->links[k].offset = k;');
		pn('\ttree->overflow_link.offset = ' + order + ';');
		pn('\tif(names != NULL) {');
		for(var n in tree['nodes']) {
			var node = tree['nodes'][n];
			if(node['name'] !== null)
				pn('\t\tnames->' + node['name'] + ' = nodes[' + node['index'] + '];');
		}
		pn('\t}');
		if(tree['overflow'] !== null) {
			pn('\ttree->overflow_node = nodes[' + tree['overflow']['index'] + '];');
			pn('\t*(int*)tree->overflow_element = ' + tree['overflow']['overflow'].value + ';');
			var l = tree['overflow']['overflow'].link;
			if(l.child !== null) {
				pn('\ttree->overflow_link.child = nodes[' + byidNode[l.child]['index'] + '];');
				pn('\ttree->overflow_link.count = ' + l.count + ';');
				pn('\ttree->overflow_link.offset = ' + l.offset + ';');
			}
		}
		for(var i = 0; i < tree['nodes'].length; i++) {
			var n = tree['nodes'][i];
			pn('\t/* Node ID: ' + n.id + ' */');
			if(n['parent'] !== null) {
				pn('\tnodes[' + i + ']->parent = nodes[' + byidNode[n['parent']].index + '];');
				if(n.childindex === null)
					pn('\tnodes[' + i + ']->child_index = ' + order + ';');
				else
					pn('\tnodes[' + i + ']->child_index = ' + n.childindex + ';');
			}
			for(var k = 0; k < order; k++) {
				var l = n['links'][k];
				if(l.child !== null) {
					pn('\tnodes[' + i + ']->links[' + k + '].child = nodes[' + byidNode[l.child]['index'] + '];');
					pn('\tnodes[' + i + ']->links[' + k + '].count = ' + l.count + ';');
					pn('\tnodes[' + i + ']->links[' + k + '].offset = ' + l.offset + ';');
				}
			}
			pn('\tnodes[' + i + ']->fill = ' + n['fill'] + ';');
			for(var k = 0; k < n['fill']; k++)
				pn('\t*((int*)nodes[' + i + ']->elements + ' + k + ') = ' + n['values'][k] + ';');
		}
		pn('\ttree->root = nodes[' + root['index'] + '];');
		pn('\treturn tree;');

		pn('}');
	}

/*	pn('\tint *value;');
	pn('\tstruct {');
	for(var n in byname)
		pn('\t\tbtree_node_t *' + n + ';');
	pn('\t} named;');*/

	pn();
	for(var t in tests) {
		var test = tests[t];
		var origin = treeForRoot(byidNode[test['origin']]);
		var expected = treeForRoot(byidNode[test['expected']]);
		if(test['name'] === null)
			addtest('ADD_TEST("[' + t + ']", run' + t + '_' + testname + ');');
		else
			addtest('ADD_TEST("' + test['name'] + '", run' + t + '_' + testname + ');');
		pn('static void run' + t + '_' + testname + '()');
		pn('{');
		pn('\tstruct names' + origin['index'] + '_' + testname + ' inames;');
		if(expected !== null)
			pn('\tstruct names' + expected['index'] + '_' + testname + ' onames;');
		pn('\tbtree_t *itree = mktree' + origin['index'] + '_' + testname + '(&inames);');
		if(expected !== null)
			pn('\tbtree_t *otree = mktree' + expected['index'] + '_' + testname + '(&onames);');
		pn('\tCU_ASSERT_PTR_NOT_NULL_FATAL(itree);');
		if(expected !== null)
			pn('\tCU_ASSERT_PTR_NOT_NULL_FATAL(otree);');

		for(var f in functions) {
			var fn = functions[f];
			pn('\t' + fn['proto']);
			pn('\t{');
			for(var b in fn['body'])
				pn('\t\t' + fn['body'][b]);
			pn('\t}');
		}

		pn('\t{');
		for(var c in test['code'])
			pn('\t\t' + test['code'][c]);
		pn('\t}');
		pn('\tCU_ASSERT_TRUE(test_tree_consistent(itree));');
		if(expected !== null)
			pn('\tCU_ASSERT_TRUE(test_tree_equal(itree, otree));');
		pn('\tbtree_destroy(itree);');
		if(expected !== null)
			pn('\tbtree_destroy(otree);');
		pn('\ttest_nodes_destroy();');
		pn('}');
	}

	if(trees.length > maxTrees)
		maxTrees = trees.length;
	if(tests.length > maxTests)
		maxTests = tests.length;
}

print('#pragma once');
print('#include "test/common.h"');
print('#include "test/btree_common.h"');
print();

var groups = [ "newroot", "split", "concatenate", "lr_redist", "rl_redist",
		"overflow_1", "overflow_2", "overflow_3", "overflow_4",
		"underflow_1", "underflow_2", "underflow_3", "underflow_4", "underflow_5",
		"find_lower", "find_upper",
		"append", "prepend",
		"poll", "pop",
		"find_index" ];

var sections = {};
var suites = {};

for(var g in groups) {
	lines = [];
	addtests = [];
	load(groups[g]);
	sections[g] = lines;
	suites[g] = addtests;
}

for(var s in sections) {
	var lines = sections[s];
	for(var l in lines)
		print(lines[l]);
}

print('int btreetest_gla_btree()');
print('{');
print('\tCU_pSuite suite;');
print('\tCU_pTest test;');
print();
for(var g in groups) {
	var tests = suites[g];
	print('\tBEGIN_SUITE("BTree: ' + groups[g] + '", NULL, NULL);');
	for(var t in tests)
		print('\t\t' + tests[t]);
	print('\tEND_SUITE;');
}
print('\treturn 0;');
print('}');


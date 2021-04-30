const fs = require("fs");

// Principles
// Make the JSON DSL as simple/plain-old-data as possible.
// E.g there used to be an ability to store variables & replace paths in expected values
// to support comparing equality for properties like timestamps in changes. We now pre-compute
// everything in the expected value (since op ids are predictable if you know the actor id)
// and just set timestamps to 0 before comparisons.

// Caveats
// 1. JSON doesn't support int keys, however patches for lists may use int keys. To solve this,
// when deserializing a patch from JSON for "apply_patch", iterate through the keys, and if any
// of them have the form "KEYTOINT:X", replace the key with the integer version of "X"
// TODO: Should we just use YAML for the generated output instead?
// Nevermind, looks like YAML also doesn't support int keys

const actor_id = "1111111111111111";
const actor = actor_id;

// (from Rust version) actor1 < actor2
const other_actor_1 = "02ef21f3c9eb4087880ebedd7c4bbe43";
const other_actor_2 = "2a1d376b24f744008d4af58252d644dd";

const tests = {
  // SKIPPED:
  // - "should be an empty object by default"
  // - "should allow actorId assignment to be deferred"
  initializing: [
    {
      name: "should allow instantiating from an existing object",
      steps: [
        {
          type: "create_doc",
          params: {
            data: {
              birds: {
                wrens: 3,
                magpies: 4,
              },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: {
            birds: {
              wrens: 3,
              magpies: 4,
            },
          },
        },
      ],
    },
    {
      name: "should accept an empty object as initial state",
      steps: [
        {
          type: "create_doc",
          params: {
            data: {},
          },
        },
        {
          type: "assert_doc_equal",
          to: {},
        },
      ],
    },
  ],
  "performing changes": [
    {
      name: "should return the unmodified document if nothing changed",
      steps: [
        {
          type: "create_doc",
        },
        {
          type: "change_doc",
          trace: [],
        },
      ],
    },
    {
      name: "should set root object properties",
      steps: [
        {
          type: "create_doc",
          params: { actor_id },
        },
        {
          type: "change_doc",
          trace: [{ type: "set", path: ["bird"], value: "magpie" }],
        },
        {
          type: "assert_doc_equal",
          to: { bird: "magpie" },
        },
        {
          type: "assert_change_equal",
          to: {
            actor: "1111111111111111",
            seq: 1,
            time: 0,
            message: "",
            startOp: 1,
            deps: [],
            ops: [
              {
                obj: "_root",
                action: "set",
                key: "bird",
                insert: false,
                value: "magpie",
                pred: [],
              },
            ],
          },
        },
      ],
    },
    {
      name: "should create nested maps",
      steps: [
        {
          type: "create_doc",
          params: { actor_id },
        },
        {
          type: "change_doc",
          trace: [{ type: "set", path: ["birds"], value: { wrens: 3 } }],
        },
        {
          type: "assert_doc_equal",
          to: {
            birds: { wrens: 3 },
          },
        },
        {
          type: "assert_change_equal",
          to: {
            actor,
            seq: 1,
            time: 0,
            message: "",
            startOp: 1,
            deps: [],
            ops: [
              {
                obj: "_root",
                action: "makeMap",
                key: "birds",
                insert: false,
                pred: [],
              },
              {
                obj: `1@${actor}`,
                action: "set",
                key: "wrens",
                insert: false,
                value: 3,
                pred: [],
              },
            ],
          },
        },
      ],
    },
    {
      name: "should apply updates inside nested maps",
      steps: [
        { type: "create_doc", params: { actor_id } },
        {
          type: "change_doc",
          trace: [{ type: "set", path: ["birds"], value: { wrens: 3 } }],
        },
        {
          type: "assert_doc_equal",
          to: { birds: { wrens: 3 } },
        },
        {
          type: "change_doc",
          trace: [{ type: "set", path: ["birds", "sparrows"], value: 15 }],
        },
        {
          type: "assert_doc_equal",
          to: { birds: { wrens: 3, sparrows: 15 } },
        },
        {
          type: "assert_change_equal",
          to: {
            actor,
            seq: 2,
            time: 0,
            message: "",
            startOp: 3,
            deps: [],
            ops: [
              {
                obj: "1@1111111111111111",
                action: "set",
                key: "sparrows",
                insert: false,
                value: 15,
                pred: [],
              },
            ],
          },
        },
      ],
    },
    {
      name: "should delete keys in maps",
      steps: [
        // NOTE: I'm using the `data` parameter even though the JS
        // tests use 2 steps. It's just more concise. I think this should be fine.
        {
          type: "create_doc",
          params: {
            actor_id,
            data: {
              magpies: 2,
              sparrows: 15,
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: { magpies: 2, sparrows: 15 },
        },
        {
          type: "change_doc",
          trace: [{ type: "delete", path: ["magpies"] }],
        },
        { type: "assert_doc_equal", to: { sparrows: 15 } },
        {
          type: "assert_change_equal",
          to: {
            actor,
            seq: 2,
            time: 0,
            message: "",
            startOp: 3,
            deps: [],
            ops: [
              {
                obj: "_root",
                action: "del",
                key: "magpies",
                insert: false,
                pred: ["1@1111111111111111"],
              },
            ],
          },
        },
      ],
    },
    {
      name: "should create lists",
      steps: [
        { type: "create_doc", params: { actor_id } },
        {
          type: "change_doc",
          trace: [{ type: "set", path: ["birds"], value: ["chaffinch"] }],
        },
        {
          type: "assert_doc_equal",
          to: { birds: ["chaffinch"] },
        },
        {
          type: "assert_change_equal",
          to: {
            actor,
            seq: 1,
            time: 0,
            message: "",
            startOp: 1,
            deps: [],
            ops: [
              {
                obj: "_root",
                action: "makeList",
                key: "birds",
                insert: false,
                pred: [],
              },
              {
                obj: `1@${actor}`,
                action: "set",
                elemId: "_head",
                insert: true,
                value: "chaffinch",
                pred: [],
              },
            ],
          },
        },
      ],
    },
    {
      // This test did not exist in the JS test suite, but I found it necessary
      name: "should insert into lists",
      steps: [
        { type: "create_doc", params: { actor_id } },
        {
          type: "change_doc",
          trace: [
            { type: "set", path: ["birds"], value: [] },
            { type: "insert", path: ["birds", 0], value: "magpie" },
            { type: "insert", path: ["birds", 1], value: "wren" },
          ],
        },
        {
          type: "assert_doc_equal",
          to: { birds: ["magpie", "wren"] },
        },
        {
          type: "assert_change_equal",
          to: {
            actor,
            seq: 1,
            startOp: 1,
            deps: [],
            time: 0,
            message: "",
            ops: [
              {
                action: "makeList",
                obj: "_root",
                key: "birds",
                insert: false,
                pred: [],
              },
              {
                action: "set",
                obj: `1@${actor}`,
                elemId: "_head",
                insert: true,
                pred: [],
                value: "magpie",
              },
              {
                action: "set",
                obj: `1@${actor}`,
                elemId: `2@${actor}`,
                insert: true,
                pred: [],
                value: "wren",
              },
            ],
          },
        },
      ],
    },
    // This test was not necessary b/c the bug was something else
    // but now that I've written it, might as well have it
    {
      name: "should insert into lists after initial list is created",
      steps: [
        { type: "create_doc", params: {actor_id, data: { foo: [] }}},
        { type: "assert_doc_equal", to: { foo: [] } },
        {
          type: "change_doc",
          trace: [
            { type: "insert", path: ["foo", 0], value: 1 },
            { type: "insert", path: ["foo", 1], value: 2 },
          ],
        },
        { type: "assert_doc_equal", to: { foo: [1, 2] } },
        {
          type: "assert_change_equal",
          to: {
            actor,
            seq: 2,
            startOp: 2,
            deps: [],
            time: 0,
            message: "",
            ops: [
              {
                action: "set",
                obj: `1@${actor}`,
                elemId: "_head",
                insert: true,
                pred: [],
                value: 1,
              },
              {
                action: "set",
                obj: `1@${actor}`,
                elemId: `2@${actor}`,
                insert: true,
                pred: [],
                value: 2,
              },
            ],
          },
        },
      ],
    },
    {
      name: "should apply updates inside lists",
      steps: [
        {
          type: "create_doc",
          params: { actor_id, data: { birds: ["chaffinch"] } },
        },
        { type: "assert_doc_equal", to: { birds: ["chaffinch"] } },
        {
          type: "change_doc",
          trace: [{ type: "set", path: ["birds", 0], value: "greenfinch" }],
        },
        { type: "assert_doc_equal", to: { birds: ["greenfinch"] } },
        {
          type: "assert_change_equal",
          to: {
            actor,
            seq: 2,
            time: 0,
            message: "",
            startOp: 3,
            deps: [],
            ops: [
              {
                obj: `1@${actor}`,
                action: "set",
                elemId: `2@${actor}`,
                insert: false,
                value: "greenfinch",
                pred: [`2@${actor}`],
              },
            ],
          },
        },
      ],
    },
    {
      name: "should delete list elements",
      steps: [
        { type: "create_doc", params: { actor_id } },
        {
          type: "change_doc",
          trace: [
            { type: "set", path: ["birds"], value: ["chaffinch", "goldfinch"] },
          ],
        },
        { type: "assert_doc_equal", to: { birds: ["chaffinch", "goldfinch"] } },
        { type: "change_doc", trace: [{ type: "delete", path: ["birds", 0] }] },
        { type: "assert_doc_equal", to: { birds: ["goldfinch"] } },
        {
          type: "assert_change_equal",
          to: {
            actor,
            seq: 2,
            time: 0,
            message: "",
            startOp: 4,
            deps: [],
            ops: [
              {
                obj: `1@${actor}`,
                action: "del",
                elemId: `2@${actor}`,
                insert: false,
                pred: [`2@${actor}`],
              },
            ],
          },
        },
      ],
    },
    {
      name: "should handle counters inside maps",
      steps: [
        { type: "create_doc", params: { actor_id } },
        {
          type: "change_doc",
          trace: [{ type: "set", path: ["wrens"], value: "VALUETOCOUNTER:0" }],
        },
        { type: "assert_doc_equal", to: { wrens: "VALUETOCOUNTER:0" } },
        {
          type: "assert_change_equal",
          to: {
            actor,
            seq: 1,
            time: 0,
            message: "",
            startOp: 1,
            deps: [],
            ops: [
              {
                obj: "_root",
                action: "set",
                key: "wrens",
                insert: false,
                value: 0,
                datatype: "counter",
                pred: [],
              },
            ],
          },
        },
        {
          type: "change_doc",
          trace: [{ type: "increment", path: ["wrens"], delta: 1 }],
        },
        { type: "assert_doc_equal", to: { wrens: "VALUETOCOUNTER:1" } },
        {
          type: "assert_change_equal",
          to: {
            actor,
            seq: 2,
            time: 0,
            message: "",
            startOp: 2,
            deps: [],
            ops: [
              {
                obj: "_root",
                action: "inc",
                key: "wrens",
                insert: false,
                value: 1,
                pred: [`1@${actor}`],
              },
            ],
          },
        },
      ],
    },
    {
      name: "should handle counters inside lists",
      steps: [
        { type: "create_doc", params: { actor_id } },
        {
          type: "change_doc",
          trace: [
            { type: "set", path: ["counts"], value: ["VALUETOCOUNTER:1"] },
          ],
        },
        {
          type: "assert_change_equal",
          to: {
            actor,
            deps: [],
            seq: 1,
            time: 0,
            message: "",
            startOp: 1,
            ops: [
              {
                obj: "_root",
                action: "makeList",
                key: "counts",
                insert: false,
                pred: [],
              },
              {
                obj: `1@${actor}`,
                action: "set",
                elemId: "_head",
                insert: true,
                value: 1,
                datatype: "counter",
                pred: [],
              },
            ],
          },
        },
        { type: "assert_doc_equal", to: { counts: ["VALUETOCOUNTER:1"] } },
        {
          type: "change_doc",
          trace: [{ type: "increment", path: ["counts", 0], delta: 2 }],
        },
        {
          type: "assert_change_equal",
          to: {
            actor,
            deps: [],
            seq: 2,
            time: 0,
            message: "",
            startOp: 3,
            ops: [
              {
                obj: `1@${actor}`,
                action: "inc",
                elemId: `2@${actor}`,
                insert: false,
                value: 2,
                pred: [`2@${actor}`],
              },
            ],
          },
        },
        { type: "assert_doc_equal", to: { counts: ["VALUETOCOUNTER:3"] } },
      ],
    },
  ],
  // SKIP: should structure share unmodified objects
  "applying patches": [
    {
      name: "should set root object properties",
      steps: [
        { type: "create_doc" },
        {
          type: "apply_patch",
          patch: {
            maxOp: 1,
            deps: [],
            clock: { "02ef21f3c9eb4087880ebedd7c4bbe43": 1 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                bird: {
                  "1@02ef21f3c9eb4087880ebedd7c4bbe43": { value: "magpie" },
                },
              },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: {
            bird: "magpie",
          },
        },
      ],
    },
    {
      name: "should reveal conflicts on root object properties",
      steps: [
        { type: "create_doc" },
        {
          type: "apply_patch",
          patch: {
            maxOp: 2,
            deps: [],
            clock: {
              "02ef21f3c9eb4087880ebedd7c4bbe43": 1,
              "2a1d376b24f744008d4af58252d644dd": 2,
            },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                favoriteBird: {
                  "1@02ef21f3c9eb4087880ebedd7c4bbe43": { value: "robin" },
                  "1@2a1d376b24f744008d4af58252d644dd": { value: "wagtail" },
                },
              },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: {
            favoriteBird: "wagtail",
          },
        },
        {
          type: "assert_conflicts_equal",
          path: ["favoriteBird"],
          to: {
            "1@02ef21f3c9eb4087880ebedd7c4bbe43": "robin",
            "1@2a1d376b24f744008d4af58252d644dd": "wagtail",
          },
        },
      ],
    },
    {
      name: "should create nested maps",
      steps: [
        { type: "create_doc" },
        {
          type: "apply_patch",
          patch: {
            maxOp: 3,
            deps: [],
            clock: { "2a1d376b24f744008d4af58252d644dd": 1 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                birds: {
                  "1@2a1d376b24f744008d4af58252d644dd": {
                    objectId: "2@2a1d376b24f744008d4af58252d644dd",
                    type: "map",
                    props: {
                      wrens: {
                        "2@2a1d376b24f744008d4af58252d644dd": { value: 3 },
                      },
                    },
                  },
                },
              },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: { birds: { wrens: 3 } },
        },
      ],
    },
    {
      name: "should apply updates inside nested maps",
      steps: [
        { type: "create_doc" },
        {
          type: "apply_patch",
          patch: {
            maxOp: 2,
            deps: [],
            clock: { "2a1d376b24f744008d4af58252d644dd": 1 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                birds: {
                  "1@2a1d376b24f744008d4af58252d644dd": {
                    // TODO: Does this make sense? Shouldn't the object id be 1@...?
                    objectId: "2@2a1d376b24f744008d4af58252d644dd",
                    type: "map",
                    props: {
                      wrens: {
                        "2@2a1d376b24f744008d4af58252d644dd": { value: 3 },
                      },
                    },
                  },
                },
              },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: { birds: { wrens: 3 } },
        },
        {
          type: "apply_patch",
          patch: {
            maxOp: 3,
            deps: [],
            clock: { "2a1d376b24f744008d4af58252d644dd": 2 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                birds: {
                  "1@2a1d376b24f744008d4af58252d644dd": {
                    objectId: "2@2a1d376b24f744008d4af58252d644dd",
                    type: "map",
                    props: {
                      sparrows: {
                        "3@2a1d376b24f744008d4af58252d644dd": { value: 15 },
                      },
                    },
                  },
                },
              },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: { birds: { wrens: 3, sparrows: 15 } },
        },
      ],
    },
    {
      name: "should apply updates inside map key conflicts",
      steps: [
        { type: "create_doc" },
        {
          type: "apply_patch",
          patch: {
            maxOp: 2,
            deps: [],
            clock: {
              "02ef21f3c9eb4087880ebedd7c4bbe43": 1,
              "2a1d376b24f744008d4af58252d644dd": 1,
            },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                favoriteBirds: {
                  "1@02ef21f3c9eb4087880ebedd7c4bbe43": {
                    objectId: "1@02ef21f3c9eb4087880ebedd7c4bbe43",
                    type: "map",
                    props: {
                      blackbirds: {
                        "2@02ef21f3c9eb4087880ebedd7c4bbe43": {
                          value: 1,
                        },
                      },
                    },
                  },
                  "1@2a1d376b24f744008d4af58252d644dd": {
                    objectId: "1@2a1d376b24f744008d4af58252d644dd",
                    type: "map",
                    props: {
                      wrens: {
                        "2@2a1d376b24f744008d4af58252d644dd": {
                          value: 3,
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: { favoriteBirds: { wrens: 3 } },
        },
        {
          type: "assert_conflicts_equal",
          path: ["favoriteBirds"],
          to: {
            "1@02ef21f3c9eb4087880ebedd7c4bbe43": { blackbirds: 1 },
            "1@2a1d376b24f744008d4af58252d644dd": { wrens: 3 },
          },
        },
        {
          type: "apply_patch",
          patch: {
            maxOp: 1,
            deps: [],
            clock: {
              "02ef21f3c9eb4087880ebedd7c4bbe43": 2,
              "2a1d376b24f744008d4af58252d644dd": 1,
            },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                favoriteBirds: {
                  "1@02ef21f3c9eb4087880ebedd7c4bbe43": {
                    objectId: "1@02ef21f3c9eb4087880ebedd7c4bbe43",
                    type: "map",
                    props: {
                      blackbirds: {
                        "3@02ef21f3c9eb4087880ebedd7c4bbe43": {
                          value: 2,
                        },
                      },
                    },
                  },
                  "1@2a1d376b24f744008d4af58252d644dd": {
                    objectId: "1@2a1d376b24f744008d4af58252d644dd",
                    type: "map",
                  },
                },
              },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: {
            favoriteBirds: { wrens: 3 },
          },
        },
        {
          type: "assert_conflicts_equal",
          path: ["favoriteBirds"],
          to: {
            "1@02ef21f3c9eb4087880ebedd7c4bbe43": { blackbirds: 2 },
            "1@2a1d376b24f744008d4af58252d644dd": { wrens: 3 },
          },
        },
      ],
    },
    {
      name: "delete keys in maps",
      steps: [
        { type: "create_doc" },
        {
          type: "apply_patch",
          patch: {
            maxOp: 2,
            deps: [],
            clock: { "02ef21f3c9eb4087880ebedd7c4bbe43": 1 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                magpies: { "1@02ef21f3c9eb4087880ebedd7c4bbe43": { value: 2 } },
                sparrows: {
                  "2@02ef21f3c9eb4087880ebedd7c4bbe43": { value: 15 },
                },
              },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: { magpies: 2, sparrows: 15 },
        },
        {
          type: "apply_patch",
          patch: {
            maxOp: 3,
            deps: [],
            clock: { "02ef21f3c9eb4087880ebedd7c4bbe43": 2 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                magpies: {},
              },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: { sparrows: 15 },
        },
      ],
    },
    {
      name: "should create lists",
      steps: [
        { type: "create_doc", params: { actor_id } },
        {
          type: "apply_patch",
          patch: {
            maxOp: 2,
            deps: [],
            clock: { [actor_id]: 1 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                birds: {
                  [`1@${actor}`]: {
                    objectId: `1@${actor}`,
                    type: "list",
                    // The JS version is missing an "elemId" prop here, but the test passes.
                    // I think this is luck, not intention, since the elemId ends up being undefined (which is def. wrong)
                    edits: [
                      { action: "insert", index: 0, elemId: `2@${actor_id}` },
                    ],
                    props: {
                      "KEYTOINT:0": {
                        [`2@${actor_id}`]: { value: "chaffinch" },
                      },
                    },
                  },
                },
              },
            },
          },
        },
        { type: "assert_doc_equal", to: { birds: ["chaffinch"] } },
      ],
    },
    {
      name: "should apply updates inside lists",
      steps: [
        { type: "create_doc", params: { actor_id } },
        {
          type: "apply_patch",
          patch: {
            maxOp: 1,
            deps: [],
            clock: { actor_id: 1 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                birds: {
                  [`1@${actor_id}`]: {
                    objectId: `1@${actor_id}`,
                    type: "list",
                    edits: [
                      { action: "insert", index: 0, elemId: `2@${actor_id}` },
                    ],
                    props: {
                      "KEYTOINT:0": {
                        [`2@${actor_id}`]: { value: "chaffinch" },
                      },
                    },
                  },
                },
              },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: { birds: ["chaffinch"] },
        },
        {
          type: "apply_patch",
          patch: {
            maxOp: 3,
            deps: [],
            clock: { actor_id: 2 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                birds: {
                  [`1@${actor_id}`]: {
                    objectId: `1@${actor_id}`,
                    type: "list",
                    edits: [],
                    props: {
                      "KEYTOINT:0": {
                        [`3@${actor_id}`]: { value: "greenfinch" },
                      },
                    },
                  },
                },
              },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: { birds: ["greenfinch"] },
        },
      ],
    },
    {
      // Copied this test from the Rust version (the JS version doesn't use proper op ids)
      name: "should apply updates inside list element conflicts",
      steps: [
        { type: "create_doc" },
        {
          type: "apply_patch",
          patch: {
            maxOp: 2,
            deps: [],
            clock: { [actor]: 1, [other_actor_1]: 1, [other_actor_2]: 1 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                birds: {
                  [`1@${actor}`]: {
                    objectId: `1@${actor}`,
                    type: "list",
                    edits: [
                      {
                        action: "insert",
                        index: 0,
                        elemId: `2@${other_actor_1}`,
                      },
                    ],
                    props: {
                      "KEYTOINT:0": {
                        [`2@${other_actor_1}`]: {
                          objectId: `2@${other_actor_1}`,
                          type: "map",
                          props: {
                            species: {
                              [`3@${other_actor_1}`]: { value: "woodpecker" },
                            },
                            numSeen: { [`4@${other_actor_1}`]: { value: 1 } },
                          },
                        },
                        [`2@${other_actor_2}`]: {
                          objectId: `2@${other_actor_2}`,
                          type: "map",
                          props: {
                            species: {
                              [`3@${other_actor_2}`]: { value: "lapwing" },
                            },
                            numSeen: { [`4@${other_actor_2}`]: { value: 2 } },
                          },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: { birds: [{ species: "lapwing", numSeen: 2 }] },
        },
        {
          type: "assert_conflicts_equal",
          path: ["birds", 0],
          to: {
            [`2@${other_actor_1}`]: {
              species: "woodpecker",
              numSeen: 1,
            },
            [`2@${other_actor_2}`]: {
              species: "lapwing",
              numSeen: 2,
            },
          },
        },
        {
          type: "apply_patch",
          patch: {
            maxOp: 5,
            deps: [],
            clock: { [other_actor_1]: 2, [other_actor_2]: 1 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                birds: {
                  [`1@${actor}`]: {
                    objectId: `1@${actor}`,
                    type: "list",
                    edits: [],
                    props: {
                      "KEYTOINT:0": {
                        [`2@${other_actor_1}`]: {
                          objectId: `2@${other_actor_1}`,
                          type: "map",
                          props: {
                            numSeen: { [`5@${other_actor_1}`]: { value: 2 } },
                          },
                        },
                        [`2@${other_actor_2}`]: {
                          objectId: `2@${other_actor_2}`,
                          type: "map",
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: {
            birds: [{ species: "lapwing", numSeen: 2 }],
          },
        },
      ],
    },
    {
      name: "should delete list elements",
      steps: [
        { type: "create_doc" },
        {
          type: "apply_patch",
          patch: {
            maxOp: 3,
            deps: [],
            clock: { [actor]: 1 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                birds: {
                  [`1@${actor}`]: {
                    objectId: `1@${actor}`,
                    type: "list",
                    edits: [
                      { action: "insert", index: 0, elemId: `2@${actor}` },
                      { action: "insert", index: 1, elemId: `3@${actor}` },
                    ],
                    props: {
                      "KEYTOINT:0": { [`2@${actor}`]: { value: "chaffinch" } },
                      "KEYTOINT:1": { [`3@${actor}`]: { value: "goldfinch" } },
                    },
                  },
                },
              },
            },
          },
        },
        { type: "assert_doc_equal", to: { birds: ["chaffinch", "goldfinch"] } },
        {
          type: "apply_patch",
          patch: {
            maxOp: 4,
            deps: [],
            clock: { [actor]: 2 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                birds: {
                  [`1@${actor}`]: {
                    objectId: `1@${actor}`,
                    type: "list",
                    props: {},
                    edits: [{ action: "remove", index: 0 }],
                  },
                },
              },
            },
          },
        },
        { type: "assert_doc_equal", to: { birds: ["goldfinch"] } },
      ],
    },
    {
      name: "should apply updates at different levels of the object tree",
      steps: [
        { type: "create_doc" },
        {
          type: "apply_patch",
          patch: {
            maxOp: 6,
            deps: [],
            clock: { [actor]: 1 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                counts: {
                  [`1@${actor}`]: {
                    objectId: `1@${actor}`,
                    type: "map",
                    props: {
                      magpies: { [`2@${actor}`]: { value: 2 } },
                    },
                  },
                },
                details: {
                  [`3@${actor}`]: {
                    objectId: `3@${actor}`,
                    type: "list",
                    edits: [
                      { action: "insert", index: 0, elemId: `4@${actor}` },
                    ],
                    props: {
                      "KEYTOINT:0": {
                        [`4@${actor}`]: {
                          objectId: `4@${actor}`,
                          type: "map",
                          props: {
                            species: { [`5@${actor}`]: { value: "magpie" } },
                            family: { [`6@${actor}`]: { value: "corvidae" } },
                          },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: {
            counts: { magpies: 2 },
            details: [{ species: "magpie", family: "corvidae" }],
          },
        },
        {
          type: "apply_patch",
          patch: {
            maxOp: 7,
            deps: [],
            clock: { [actor]: 2 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                counts: {
                  [`1@${actor}`]: {
                    objectId: `1@${actor}`,
                    type: "map",
                    props: {
                      magpies: { [`7@${actor}`]: { value: 3 } },
                    },
                  },
                },
                details: {
                  [`3@${actor}`]: {
                    objectId: `3@${actor}`,
                    type: "list",
                    edits: [],
                    props: {
                      "KEYTOINT:0": {
                        [`4@${actor}`]: {
                          objectId: `4@${actor}`,
                          type: "map",
                          props: {
                            species: {
                              [`8@${actor}`]: { value: "Eurasian magpie" },
                            },
                          },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: {
            counts: { magpies: 3 },
            details: [{ species: "Eurasian magpie", family: "corvidae" }],
          },
        },
      ],
    },
  ],
  // SKIP: dont_allow_out_of_order_request_patches (requires asserting a method throws or returns an error)
  // better to just do this test natively since it's the only assertion of this type
  // should_allow_interleaving_of_patches_and_changes,
  // deps_are_filled_in_if_the_frontend_does_not_have_the_latest_patch
  "backend concurrency": [
    {
      name: "should use version and sequence number from the backend",
      steps: [
        { type: "create_doc", params: { actor_id } },
        {
          type: "apply_patch",
          patch: {
            clock: { [actor_id]: 4, [other_actor_1]: 11, [other_actor_2]: 41 },
            maxOp: 4,
            deps: [],
            diffs: {
              objectId: "_root",
              type: "map",
              props: { blackbirds: { [actor_id]: { value: 24 } } },
            },
          },
        },
        {
          type: "change_doc",
          trace: [{ type: "set", path: ["partridges"], value: 1 }],
        },
        {
          type: "assert_change_equal",
          to: {
            actor,
            seq: 5,
            deps: [],
            startOp: 5,
            time: 0,
            message: "",
            ops: [
              {
                obj: "_root",
                action: "set",
                key: "partridges",
                insert: false,
                value: 1,
                pred: [],
              },
            ],
          },
        },
        {
          type: "assert_in_flight_equal",
          to: [{ seq: 5 }],
        },
      ],
    },
    {
      name: "should remove pending requests once handled",
      steps: [
        { type: "create_doc", params: { actor_id } },
        {
          type: "change_doc",
          trace: [{ type: "set", path: ["blackbirds"], value: 24 }],
        },
        {
          type: "assert_change_equal",
          to: {
            actor,
            seq: 1,
            deps: [],
            startOp: 1,
            time: 0,
            message: "",
            ops: [
              {
                obj: "_root",
                action: "set",
                key: "blackbirds",
                insert: false,
                value: 24,
                pred: [],
              },
            ],
          },
        },
        {
          type: "change_doc",
          trace: [{ type: "set", path: ["partridges"], value: 1 }],
        },
        {
          type: "assert_change_equal",
          to: {
            actor,
            seq: 2,
            deps: [],
            startOp: 2,
            time: 0,
            message: "",
            ops: [
              {
                obj: "_root",
                action: "set",
                key: "partridges",
                insert: false,
                value: 1,
                pred: [],
              },
            ],
          },
        },
        {
          type: "assert_in_flight_equal",
          to: [{ seq: 1 }, { seq: 2 }],
        },
        {
          type: "apply_patch",
          patch: {
            actor,
            seq: 1,
            maxOp: 4,
            deps: [],
            clock: { [actor]: 1 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: { blackbirds: { [actor]: { value: 24 } } },
            },
          },
        },
        {
          type: "assert_in_flight_equal",
          to: [{ seq: 2 }],
        },
        {
          type: "assert_doc_equal",
          to: { blackbirds: 24, partridges: 1 },
        },
        {
          type: "apply_patch",
          patch: {
            actor,
            seq: 2,
            maxOp: 5,
            deps: [],
            clock: { [actor]: 2 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: { partridges: { [actor]: { value: 1 } } },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: { blackbirds: 24, partridges: 1 },
        },
        {
          type: "assert_in_flight_equal",
          to: [],
        },
      ],
    },
    {
      name: "should leave the request queue unchanged on remote patches",
      steps: [
        { type: "create_doc", params: { actor_id } },
        {
          type: "change_doc",
          trace: [{ type: "set", path: ["blackbirds"], value: 24 }],
        },
        {
          type: "assert_change_equal",
          to: {
            actor,
            seq: 1,
            deps: [],
            startOp: 1,
            time: 0,
            message: "",
            ops: [
              {
                obj: "_root",
                action: "set",
                key: "blackbirds",
                insert: false,
                value: 24,
                pred: [],
              },
            ],
          },
        },
        {
          type: "assert_in_flight_equal",
          to: [{ seq: 1 }],
        },
        {
          type: "apply_patch",
          patch: {
            maxOp: 10,
            deps: [],
            clock: { [other_actor_1]: 1 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: { pheasants: { [other_actor_1]: { value: 2 } } },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: { blackbirds: 24 },
        },
        {
          type: "assert_in_flight_equal",
          to: [{ seq: 1 }],
        },
        {
          type: "apply_patch",
          patch: {
            actor,
            seq: 1,
            maxOp: 11,
            deps: [],
            clock: { [actor]: 1, [other_actor_1]: 1 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: { blackbirds: { [actor]: { value: 24 } } },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: { blackbirds: 24, pheasants: 2 },
        },
        {
          type: "assert_in_flight_equal",
          to: [],
        },
      ],
    },
    {
      name: "should handle concurrent insertions into lists",
      steps: [
        { type: "create_doc", params: { actor_id } },
        {
          type: "change_doc",
          trace: [{ type: "set", path: ["birds"], value: ["goldfinch"] }],
        },
        {
          type: "apply_patch",
          patch: {
            actor,
            seq: 1,
            maxOp: 1,
            deps: [],
            clock: { [actor]: 1 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                // TODO: I think these actor ids are wrong (even though
                // they are copied from the Rust tests)
                birds: {
                  [`1@${actor}`]: {
                    objectId: `1@${actor}`,
                    type: "list",
                    edits: [
                      { action: "insert", index: 0, elemId: `1@${actor}` },
                    ],
                    props: {
                      "KEYTOINT:0": { [`1@${actor}`]: { value: "goldfinch" } },
                    },
                  },
                },
              },
            },
          },
        },
        { type: "assert_doc_equal", to: { birds: ["goldfinch"] } },
        { type: "assert_in_flight_equal", to: [] },
        {
          type: "change_doc",
          trace: [
            { type: "insert", path: ["birds", 0], value: "chaffinch" },
            { type: "insert", path: ["birds", 2], value: "greenfinch" },
          ],
        },
        {
          type: "apply_patch",
          patch: {
            clock: { [actor]: 1, [other_actor_1]: 1 },
            deps: [],
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                birds: {
                  [`1@${actor}`]: {
                    objectId: `1@${actor}`,
                    type: "list",
                    edits: [
                      {
                        action: "insert",
                        index: 1,
                        elemId: `1${other_actor_1}`,
                      },
                    ],
                    props: {
                      "KEYTOINT:1": {
                        [`1@${other_actor_1}`]: { value: "bullfinch" },
                      },
                    },
                  },
                },
              },
            },
          },
        },
        {
          type: "assert_doc_equal",
          to: { birds: ["chaffinch", "goldfinch", "greenfinch"] },
        },
        {
          type: "apply_patch",
          patch: {
            actor,
            seq: 2,
            maxOp: 3,
            deps: [],
            clock: { [actor]: 2, [other_actor_1]: 1 },
            diffs: {
              objectId: "_root",
              type: "map",
              props: {
                birds: {
                  [`1@${actor}`]: {
                    objectId: `1@${actor}`,
                    type: "list",
                    edits: [
                      { action: "insert", index: 0, elemId: `1@${actor}` },
                      { action: "insert", index: 2, elemId: `2@${actor}` },
                    ],
                    props: {
                      "KEYTOINT:0": { [`2@${actor}`]: { value: "chaffinch" } },
                      "KEYTOINT:2": { [`3@${actor}`]: { value: "greenfinch" } },
                    },
                  },
                },
              },
            },
          },
        },
        { type: "assert_in_flight_equal", to: [] },
        {
          type: "assert_doc_equal",
          to: { birds: ["chaffinch", "goldfinch", "greenfinch", "bullfinch"] },
        },
      ],
    },
  ],
};

const json = JSON.stringify(tests, null, 2);
fs.writeFileSync("frontend_tests.json", json);

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
                      'KEYTOINT:0': { [`2@${actor}`]: { value: "chaffinch" } },
                      'KEYTOINT:1': { [`3@${actor}`]: { value: "goldfinch" } },
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
  ],
};

const json = JSON.stringify(tests, null, 2);
fs.writeFileSync("frontend_tests.json", json);

const fs = require("fs");

// Principles
// Make the JSON DSL as simple/plain-old-data as possible.
// E.g there used to be an ability to store variables & replace paths in expected values
// to support comparing equality for properties like timestamps in changes. We now pre-compute
// everything in the expected value (since op ids are predictable if you know the actor id)
// and just set timestamps to 0 before comparisons.

const actor_id = "1111111111111111";
const actor = actor_id;

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
        {
          type: "assert_doc_equal",
          to: { sparrows: 15 },
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
                        "2@02ef21f3c9eb4087880ebedd7c4bbe43": { value: 1 },
                      },
                    },
                  },
                  "1@2a1d376b24f744008d4af58252d644dd": {
                    objectId: "1@2a1d376b24f744008d4af58252d644dd",
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
          to: { favoriteBirds: { wrens: 3 } },
        },
        {
          type: "assert_conflicts_equal",
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
                        "3@02ef21f3c9eb4087880ebedd7c4bbe43": { value: 2 },
                      },
                    },
                  },
                  "1@2a1d376b24f744008d4af58252d644dd": {
                    objectId: "1@02ef21f3c9eb4087880ebedd7c4bbe43",
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
  ],
};

const json = JSON.stringify(tests, null, 2);
fs.writeFileSync("frontend_tests.json", json);

var MemoryStore = require('logux-core/memory-store')
var sizeof = require('object-sizeof')

/**
 * Extension for logux memory-based events store.
 *
 * It uses memory store on top, but when memory store reaches
 * entries count or memory constraint – flushes data to disk.
 * Data can be aquired by additional pagination methods on object,
 * returned by get method.
 *
 * @example
 * import { FileStore } from 'logux-file-store'
 *
 * var log = new Log({
 *   store: new FileStore(entriesLimit: 10000, memoryLimit: 1048576 * 100),
 *   timer: createTestTimer()
 * })
 *
 * @class
 * @extends MemoryStore
 */

function FileStore (opts) {
  this.memoryStore = new MemoryStore()
  this.filesTable = { }

  if (!opts) opts = { }

  this.entriesLimit = opts.entriesLimit || 10000
  this.memoryLimit = opts.memoryLimit || 1048576 * 100 // 100MiB
  this.addedCount = 0
  this.addedMemory = 0
}

FileStore.prototype = {

  get: function get (order) {
    // Wrap with paged file reading object with
    // first result from memory store
    return this.memoryStore.get(order)
  },

  add: function add (entry) {
    if (this.addedCount === this.entriesLimit) {
      this.flush()
    }
    this.addedCount += 1
    this.addedMemory += sizeof(entry)
    return this.memoryStore.add(entry)
  },

  flush: function flush () {
    // dump added and created to files
    this.memoryStore.added = []
    this.memoryStore.created = []
    this.addedCount = 0
    this.addedMemory = 0
  },

  needFlush: function needFlush () {
    return this.addedCount === this.entriesLimit ||
      (this.addedCount > (this.entriesLimit / 2) &&
       this.addedMemory >= this.memoryLimit)
  }

  search: function search (time) {
    return this.memoryStore.search(time)
  }

  remove: function remove(time) {
    // Do we need to add remove from dumped files?
    // If so – we can create aditional array for `toRemove` entries
    // which will be used in compaction stage
    return this.memoryStore.remove(time)
  }

}

module.exports = FileStore

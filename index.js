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
 */

function FileStore (opts) {
  this.memoryStore = new MemoryStore()
  this.filesTable = {
    lastAddedIndex: 0,
    lastCreatedIndex: 0,
  }

  if (!opts) opts = { }

  this.entriesLimit = opts.entriesLimit || 10000
  this.memoryLimit = opts.memoryLimit || 1048576 * 100 // 100MiB
  this.filesPath = opts.path || '.'
  this.addedCount = 0
  this.addedMemory = 0
}

FileStore.prototype = {

  get: function get (order) {
    // Wrap with paged file reading object with
    // first result from memory store
    return this.memoryStore.get(order)
  },

  // Because in memory store two arrays we multiply object size by 2
  add: function add (entry) {
    if (this.addedCount === this.entriesLimit) {
      return this.flush().then(() => {
        this.addedCount += 1 * 2
        this.addedMemory += sizeof(entry) * 2
        return this.memoryStore.add(entry)
      })
    } else {
      this.addedCount += 1 * 2
      this.addedMemory += sizeof(entry) * 2
      return this.memoryStore.add(entry)
    }
  },

  flush: function flush () {
    return new Promise(resolve, reject) {
      // Added first
      var addedIndex = ++this.filesTable.lastAddedIndex
      var addedPath = `${this.filesPath}/added_${fileIndex}.log`
      var addedContent = this.memoryStore.added.map(entry => {
        return JSON.stringify(entry)
      }).join("\n")
      fs.writeFile(addedPath, addedContent, function (err) {
        if (err) {
          reject(err)
        } else {
          this.memoryStore.added = []
          this.addedMemory = this.addedMemory / 2
          this.addedCount = this.addedCount / 2

          var createdIndex = ++this.filesTable.lastCreatedIndex
          var createdPath = `${this.filesPath}/created_${fileIndex}.log`
          var createdContent = this.memoryStore.created.map(entry => {
            return JSON.stringify(entry)
          }).join("\n")

          fs.writeFile(createdPath, createdContent, function (err) {
            if (err) {
              reject(err)
            } else {
              this.memoryStore.created = []
              this.addedCount = 0
              this.addedMemory = 0
              resolve()
            }
          })
        }
      }
    }
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

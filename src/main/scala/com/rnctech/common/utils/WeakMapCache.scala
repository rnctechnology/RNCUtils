package com.rnctech.common.utils

import java.util

class WeakMapCache[K, V <: AnyRef](valueExtractor: K => V, bulkExtractor: K => Iterable[(K, V)] = null, default : K => V = (_ : K) => null.asInstanceOf[V]) {
    protected val cache : java.util.Map[K, Option[V]] = new util.WeakHashMap[K, Option[V]]()

    def get(key: K): V = getOpt(key).getOrElse(null.asInstanceOf[V])

    def getKeys: util.Set[K] = cache.keySet

    private def convertValue(keyvalue:(K, V)) : (K, V)= (keyvalue._1, convertValue(keyvalue._1, keyvalue._2))

    protected def preFilter(key: K): Boolean = true

    protected def convertValue(key: K, value: V): V = value

    def getOpt(key: K): Option[V] = {
      var value = cache.get(key)
      if (null == value) {
        this.synchronized {
          value = cache.get(key)
          if (null == value) {
            if (null != bulkExtractor) {
              if (preFilter(key)) {
                bulkExtractor(key).
                  toStream.map(convertValue).foreach(keyvalue => {
                  if(key == keyvalue._1)
                    value = Option(keyvalue._2)
                  cache.put(keyvalue._1, Option(keyvalue._2))
              }
                )
                if (null == value) {
                  value = None
                cache.put(key, None)
              }
            } else {
                value = None
            }
          } else {
              value = Option(valueExtractor(key)).map(convertValue(key, _))
              cache.put(key, value)
            }
          }
        }
      }
      value.orElse(Option(default(key)))
    }


}

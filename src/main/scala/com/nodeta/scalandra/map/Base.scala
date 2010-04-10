package com.nodeta.scalandra.map

import com.nodeta.scalandra.Client

trait Base[A, B ,C] {
  protected val client : Client[A, B, C]
}

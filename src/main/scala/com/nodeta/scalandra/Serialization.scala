package com.nodeta.scalandra

import serializer.Serializer

case class Serialization[A, B, C](superColumn : Serializer[A],
                                  column : Serializer[B],
                                  value : Serializer[C]) {}

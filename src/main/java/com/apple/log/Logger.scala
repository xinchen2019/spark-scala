package com.apple.log

trait Logger {
  def log(message: String) = println(message)
}
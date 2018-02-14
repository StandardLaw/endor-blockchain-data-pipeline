package com.endor.storage.sources

trait HasPath {
  def path: String
}

trait HasSubPaths extends HasPath {
  def /(subPath: String): FileSource = {
    require(!subPath.contains("/"))

    new FileSource(s"$path/$subPath")
  }
}

package dev.helight.kodein.spec

interface FieldNameProducer {
    fun getFieldsNames(): Collection<String>
}
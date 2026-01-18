package dev.helight.kodein.spec

interface FieldNameProducer {
    fun getFieldsNames(): Collection<String>

    companion object {
        fun fromNames(vararg names: String): FieldNameProducer {
            return object : FieldNameProducer {
                override fun getFieldsNames(): Collection<String> = names.toList()
            }
        }
    }
}
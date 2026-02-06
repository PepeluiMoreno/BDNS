/* apps\frontend\src\components\ConvocatoriaFilter.vue */

<script setup>
import { ref, computed } from 'vue';

const emit = defineEmits(['filter']);

const filters = ref({
  titulo: '',
  organo: '',
  anio: new Date().getFullYear(),
  importe_minimo: '',
  importe_maximo: '',
  tipo_ayuda: '',
});

const anos = computed(() => {
  const currentYear = new Date().getFullYear();
  const years = [];
  for (let i = 2015; i <= currentYear; i++) {
    years.push(i);
  }
  return years.reverse();
});

const tiposAyuda = [
  'Subvención',
  'Préstamo',
  'Garantía',
  'Crédito',
  'Otro',
];

const applyFilters = () => {
  const activeFilters = {};
  if (filters.value.titulo) activeFilters.titulo = filters.value.titulo;
  if (filters.value.organo) activeFilters.organo = filters.value.organo;
  if (filters.value.anio) activeFilters.anio = filters.value.anio;
  if (filters.value.importe_minimo) activeFilters.importe_minimo = parseFloat(filters.value.importe_minimo);
  if (filters.value.importe_maximo) activeFilters.importe_maximo = parseFloat(filters.value.importe_maximo);
  if (filters.value.tipo_ayuda) activeFilters.tipo_ayuda = filters.value.tipo_ayuda;
  
  emit('filter', activeFilters);
};

const resetFilters = () => {
  filters.value = {
    titulo: '',
    organo: '',
    anio: new Date().getFullYear(),
    importe_minimo: '',
    importe_maximo: '',
    tipo_ayuda: '',
  };
  applyFilters();
};
</script>

<template>
  <div class="bg-white rounded-lg shadow-md p-6 mb-6">
    <div class="flex items-center justify-between mb-6">
      <h2 class="text-xl font-semibold text-slate-900 flex items-center gap-2">
        <svg class="w-5 h-5 text-primary-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
        </svg>
        Filtros de Búsqueda
      </h2>
      <button
        @click="resetFilters"
        class="text-sm px-3 py-1 text-slate-600 hover:text-slate-900 hover:bg-slate-100 rounded transition-colors"
      >
        Limpiar filtros
      </button>
    </div>

    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-6">
      <!-- Título -->
      <div>
        <label class="block text-sm font-medium text-slate-700 mb-2">Título de Convocatoria</label>
        <input
          v-model="filters.titulo"
          type="text"
          placeholder="Buscar por título..."
          class="w-full px-3 py-2 border border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
        />
      </div>

      <!-- Órgano Concedente -->
      <div>
        <label class="block text-sm font-medium text-slate-700 mb-2">Órgano Concedente</label>
        <input
          v-model="filters.organo"
          type="text"
          placeholder="Buscar por órgano..."
          class="w-full px-3 py-2 border border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
        />
      </div>

      <!-- Año -->
      <div>
        <label class="block text-sm font-medium text-slate-700 mb-2">Año</label>
        <select
          v-model.number="filters.anio"
          class="w-full px-3 py-2 border border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
        >
          <option value="">Todos los años</option>
          <option v-for="year in anos" :key="year" :value="year">{{ year }}</option>
        </select>
      </div>

      <!-- Importe Mínimo -->
      <div>
        <label class="block text-sm font-medium text-slate-700 mb-2">Importe Mínimo (€)</label>
        <input
          v-model="filters.importe_minimo"
          type="number"
          placeholder="0"
          min="0"
          class="w-full px-3 py-2 border border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
        />
      </div>

      <!-- Importe Máximo -->
      <div>
        <label class="block text-sm font-medium text-slate-700 mb-2">Importe Máximo (€)</label>
        <input
          v-model="filters.importe_maximo"
          type="number"
          placeholder="Sin límite"
          min="0"
          class="w-full px-3 py-2 border border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
        />
      </div>

      <!-- Tipo de Ayuda -->
      <div>
        <label class="block text-sm font-medium text-slate-700 mb-2">Tipo de Ayuda</label>
        <select
          v-model="filters.tipo_ayuda"
          class="w-full px-3 py-2 border border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
        >
          <option value="">Todos los tipos</option>
          <option v-for="tipo in tiposAyuda" :key="tipo" :value="tipo">{{ tipo }}</option>
        </select>
      </div>
    </div>

    <!-- Botón de búsqueda -->
    <div class="flex gap-3 justify-end">
      <button
        @click="applyFilters"
        class="px-6 py-2 bg-primary-600 text-white rounded-lg hover:bg-primary-700 transition-colors font-medium flex items-center gap-2"
      >
        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
        </svg>
        Buscar
      </button>
    </div>
  </div>
</template>
<template>
  <div class="bg-white rounded-lg shadow-md p-6 border-l-4 border-primary-500 hover:shadow-lg transition-shadow">
    <div class="flex items-start justify-between">
      <div>
        <p class="text-sm font-medium text-slate-600 mb-1">{{ label }}</p>
        <p class="text-3xl font-bold text-slate-900">{{ formattedValue }}</p>
        <p v-if="subtitle" class="text-xs text-slate-500 mt-2">{{ subtitle }}</p>
      </div>
      <div v-if="icon" class="w-12 h-12 bg-primary-100 rounded-lg flex items-center justify-center">
        <component :is="icon" class="w-6 h-6 text-primary-600" />
      </div>
    </div>
    <div v-if="trend" class="mt-4 flex items-center gap-2">
      <span :class="[
        'inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium',
        trend > 0 ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
      ]">
        <svg class="w-3 h-3 mr-1" :class="trend > 0 ? 'text-green-600' : 'text-red-600'" fill="currentColor" viewBox="0 0 20 20">
          <path v-if="trend > 0" fill-rule="evenodd" d="M12 7a1 1 0 110-2h5a1 1 0 011 1v5a1 1 0 11-2 0V8.414l-4.293 4.293a1 1 0 01-1.414-1.414L13.586 7H12z" clip-rule="evenodd" />
          <path v-else fill-rule="evenodd" d="M12 13a1 1 0 110 2H7a1 1 0 01-1-1V9a1 1 0 112 0v3.586l4.293-4.293a1 1 0 011.414 1.414L8.414 13H12z" clip-rule="evenodd" />
        </svg>
        {{ Math.abs(trend) }}%
      </span>
      <span class="text-xs text-slate-500">vs año anterior</span>
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue';

const props = defineProps({
  label: {
    type: String,
    required: true,
  },
  value: {
    type: [Number, String],
    required: true,
  },
  subtitle: {
    type: String,
    default: null,
  },
  icon: {
    type: Object,
    default: null,
  },
  trend: {
    type: Number,
    default: null,
  },
  format: {
    type: String,
    enum: ['currency', 'number', 'text'],
    default: 'text',
  },
});

const formattedValue = computed(() => {
  const val = props.value;
  
  if (props.format === 'currency') {
    return new Intl.NumberFormat('es-ES', {
      style: 'currency',
      currency: 'EUR',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(val);
  }
  
  if (props.format === 'number') {
    return new Intl.NumberFormat('es-ES').format(val);
  }
  
  return val;
});
</script>

<style scoped>
</style>


# Copyright (c) 2026, Worlber

worlber_ha_sources = files(
  'worlber_ha.c',
)

if host_system == 'windows'
  worlber_ha_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'worlber_ha',
    '--FILEDESC', 'worlber_ha - availability groups for PostgreSQL managed from SQL',])
endif

worlber_ha = shared_module('worlber_ha',
  worlber_ha_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += worlber_ha

install_data(
  'worlber_ha--1.0.sql',
  'worlber_ha--1.0--1.1.sql',
  'worlber_ha--1.1--1.2.sql',
  'worlber_ha.control',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'worlber_ha',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'worlber_ha',
    ],
  },
  'tap': {
    'tests': [
      't/001_cluster.pl',
    ],
  },
}

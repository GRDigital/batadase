#![allow(dead_code)]

use proc_quote::quote;

#[proc_macro_derive(DbName, attributes(name, flags, table))]
pub fn derive_db_name(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
	let input = syn::parse_macro_input!(input as syn::DeriveInput);
	let name = &input.ident;

	let mut db_name = None;
	let mut flags = None;
	let mut table = None;
	for attr in input.attrs {
		let args = attr.meta.require_list().unwrap();
		match &attr.path().get_ident().unwrap().to_string() as &str {
			"name" => {
				let lit = args.parse_args::<syn::LitStr>().unwrap();
				db_name = Some(syn::LitByteStr::new(format!("{}\0", lit.value()).as_bytes(), lit.span()));
			},
			"flags" => { flags = Some(args.parse_args::<syn::Expr>().unwrap()); },
			"table" => { table = Some(args.parse_args::<syn::Type>().unwrap()); },
			_ => unreachable!(),
		}
	}

	let flags = flags.map_or_else(|| quote!(), |x| quote!(fn flags() -> ::batadase::enumflags2::BitFlags<::batadase::lmdb::DbFlags> { #x.into() }));
	let db_name = db_name.map_or_else(|| quote!(&::std::concat!(::std::module_path!(), "::", ::std::stringify!(#name), "\0").as_bytes()), |x| quote!(#x));//syn::LitByteStr::new(format!("{}\0", name).as_bytes(), name.span()));

	quote!(
		impl ::batadase::DbName for #name {
			type Table<'tx, TX: ::batadase::transaction::Transaction> = #table;
			const NAME: &'static [u8] = #db_name;

			fn get<TX: ::batadase::transaction::Transaction>(tx: &TX) -> Self::Table<'_, TX> { Self::Table::build(tx, crate::db::ENV.dbs[Self::NAME]) }
			#flags
		}
	).into()
}

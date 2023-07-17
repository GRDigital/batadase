use std::marker::PhantomData;

use crate::prelude::*;

#[derive(Serialize, Deserialize, Shrinkwrap)]
#[serde(transparent)]
pub struct Index<T>(#[shrinkwrap(main_field)] u64, #[serde(skip)] PhantomData<T>);

impl<T> rkyv::Archive for Index<T> {
	type Archived = Index<T>;
	type Resolver = ();

	#[inline]
	unsafe fn resolve(&self, _: usize, _: (), out: *mut Self::Archived) {
		out.write(*self);
	}
}

impl<D: rkyv::Fallible + ?Sized, T> rkyv::Deserialize<Index<T>, D> for rkyv::Archived<Index<T>> {
	#[inline]
	fn deserialize(&self, _: &mut D) -> Result<Index<T>, D::Error> {
		Ok(*self)
	}
}

impl<S: rkyv::Fallible + ?Sized, T> rkyv::Serialize<S> for Index<T> {
	#[inline]
	fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
		rkyv::Serialize::serialize(&self.0, serializer)
	}
}

impl<T> std::fmt::Debug for Index<T> {
	fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "Index<{}>({})", std::any::type_name::<T>(), self.0) }
}

impl<T> From<Index<T>> for usize {
	fn from(value: Index<T>) -> Self { value.0 as usize }
}

impl<T> From<Index<T>> for u64 {
	fn from(value: Index<T>) -> Self { value.0 }
}

impl<T> From<u64> for Index<T> {
	fn from(value: u64) -> Self { Self(value, PhantomData) }
}

impl<T> From<usize> for Index<T> {
	fn from(value: usize) -> Self { Self(value as u64, PhantomData) }
}

impl<T> From<u32> for Index<T> {
	fn from(value: u32) -> Self { Self(u64::from(value), PhantomData) }
}

impl<T> Clone for Index<T> {
	fn clone(&self) -> Self { *self }
}

impl<T> PartialEq for Index<T> {
	fn eq(&self, other: &Self) -> bool { self.0 == other.0 }
}

impl<T> Eq for Index<T> {}

impl<T> std::hash::Hash for Index<T> {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) { self.0.hash(state); }
}

impl<T> Copy for Index<T> {}

impl<T> std::string::ToString for Index<T> {
	fn to_string(&self) -> String { self.0.to_string() }
}

impl<T> std::cmp::PartialOrd for Index<T> {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { self.0.partial_cmp(&other.0) }
}

impl<T> std::cmp::Ord for Index<T> {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.0.cmp(&other.0) }
}

impl<T> Default for Index<T> {
	fn default() -> Self { Self(0, PhantomData) }
}

#[cfg(feature = "inspect_control")]
pub mod control {
	use inspect::{Control, ControlBuilder};
	use hobo::prelude::*;
	use error;

	use super::*;

	pub type Resolver<T> = fn(Index<T>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<T>, error::Error>>>>;

	impl<'a, T> Control<'a> for Index<T> where
		T: for<'b> Control<'b> + std::fmt::Debug + 'static,
	{
		type Builder = IndexControlBuilder<'a, T>;
		type Element = hobo::create::Div;
	}

	pub struct IndexControlBuilder<'a, T> where
		T: for<'b> Control<'b> + std::fmt::Debug + 'static,
	{
		pub value: &'a Index<T>,
	}

	impl<'a, T> ControlBuilder<'a, Index<T>> for IndexControlBuilder<'a, T> where
		T: for<'b> Control<'b> + std::fmt::Debug + 'static,
	{
		fn new(value: &'a Index<T>) -> Self { Self { value } }

		fn build(self) -> <Index<T> as inspect::Control<'a>>::Element {
			let id = *self.value;

			let element = hobo::create::div();
			element
				.class(css::Display::Flex)
				.child(format!("{:?}", id).control().build().class(css::margin_right!(15 px)))
				.child(hobo::create::div()
					.class((
						css::Cursor::Pointer,
						css::padding_horizontal!(15 px),
						css::background_color!(css::color::PALEVIOLETRED),
					))
					.text("resolve")
					.on_click(move |_| {
						#[cfg(target_arch = "wasm32")]
						wasm_bindgen_futures::spawn_local(async move {
							let resolver = Resolver::<T>::resource();
							match resolver(id).await {
								Ok(x) => { element.replace_with(x.control().build()); },
								Err(e) => element.set_text(format!("Error: {:?}", e)),
							}
						});
					})
				)
		}
	}
}
